import {
  KafkaTopicMutableConfigKeys,
  type KafkaTopicCatalogEntry,
  getKafkaTopicReconciliationCatalog,
  validateKafkaTopicCatalog,
} from "@omnixys/kafka";
import { setTimeout as sleep } from "node:timers/promises";
import {
  RpkClient,
  commandSucceeded,
  topicMissing,
  type CommandResult,
} from "./rpk.js";
import type { TopicManagerConfig } from "./config.js";

export interface TopicAdminClient {
  clusterInfo(): Promise<CommandResult>;
  describeTopic(topic: string): Promise<CommandResult>;
  createTopic(input: {
    topic: string;
    partitions: number;
    replicas: number;
    config: Record<string, string | number | boolean>;
    dryRun?: boolean;
  }): Promise<CommandResult>;
  alterTopicConfig(
    topic: string,
    config: Record<string, string | number | boolean>,
    dryRun?: boolean,
  ): Promise<CommandResult>;
}

export interface TopicManagerSummary {
  total: number;
  created: number;
  updated: number;
  skipped: number;
  warnings: number;
  errors: number;
}

export interface TopicManagerLogger {
  log(message: string): void;
  warn(message: string): void;
  error(message: string): void;
}

export const consoleLogger: TopicManagerLogger = {
  log: (message) => console.log(message),
  warn: (message) => console.warn(message),
  error: (message) => console.error(message),
};

export async function reconcileTopics(
  config: TopicManagerConfig,
  rpk: TopicAdminClient = new RpkClient(config.brokers, config.rpkConfigOptions),
  logger: TopicManagerLogger = consoleLogger,
): Promise<TopicManagerSummary> {
  const catalog = getKafkaTopicReconciliationCatalog();
  const validation = validateKafkaTopicCatalog(catalog);
  const summary: TopicManagerSummary = {
    total: catalog.topics.length,
    created: 0,
    updated: 0,
    skipped: 0,
    warnings: validation.warnings.length,
    errors: validation.errors.length,
  };

  for (const warning of validation.warnings) {
    logger.warn(`Validation warning: ${warning}`);
  }

  if (!validation.valid) {
    for (const error of validation.errors) {
      logger.error(`Validation error: ${error}`);
    }
    printSummary(summary, logger);
    throw new Error("Kafka topic catalog validation failed.");
  }

  await waitForBroker(config, rpk, logger);

  for (const topic of catalog.topics) {
    logger.log("");
    logger.log(`Reconciling topic: ${topic.topic}`);

    const describe = await rpk.describeTopic(topic.topic);

    if (topicMissing(describe)) {
      logger.log(`Creating topic: ${topic.topic}`);
      const create = await rpk.createTopic({
        topic: topic.topic,
        partitions: topic.partitions,
        replicas: topic.replicas,
        config: topic.config,
        dryRun: config.dryRun,
      });

      if (!commandSucceeded(create)) {
        summary.errors += 1;
        logger.error(`Create failed: ${formatCommandError(create)}`);
        continue;
      }

      summary.created += 1;
      continue;
    }

    if (!commandSucceeded(describe)) {
      summary.errors += 1;
      logger.error(`Describe failed: ${formatCommandError(describe)}`);
      continue;
    }

    logger.log(`Already exists: ${topic.topic}`);
    const existing = parseDescribeOutput(describe.stdout);
    const immutableWarnings = detectImmutableDrift(topic, existing);
    const mutableDrift = detectMutableConfigDrift(topic, existing.config);

    for (const warning of immutableWarnings) {
      summary.warnings += 1;
      logger.warn(warning);
    }

    if (mutableDrift.length === 0) {
      summary.skipped += 1;
      continue;
    }

    for (const drift of mutableDrift) {
      logger.log(
        `Updating ${drift.key}: desired=${drift.desired} current=${drift.current ?? "<unset>"}`,
      );
    }

    if (!config.mutableConfig) {
      summary.warnings += 1;
      logger.warn(`Mutable drift detected but reconciliation is disabled.`);
      summary.skipped += 1;
      continue;
    }

    const update = await rpk.alterTopicConfig(
      topic.topic,
      Object.fromEntries(mutableDrift.map((drift) => [drift.key, drift.desired])),
      config.dryRun,
    );

    if (!commandSucceeded(update)) {
      summary.errors += 1;
      logger.error(`Update failed: ${formatCommandError(update)}`);
      continue;
    }

    summary.updated += 1;
  }

  printSummary(summary, logger);

  if (summary.errors > 0) {
    throw new Error("Kafka topic reconciliation finished with errors.");
  }

  return summary;
}

export interface ParsedTopicDescription {
  partitions?: number;
  replicas?: number;
  config: Record<string, string>;
}

export interface MutableConfigDrift {
  key: string;
  desired: string;
  current?: string;
}

export function detectMutableConfigDrift(
  desired: KafkaTopicCatalogEntry,
  currentConfig: Record<string, string>,
): MutableConfigDrift[] {
  return KafkaTopicMutableConfigKeys.flatMap((key) => {
    const desiredValue = desired.config[key];
    if (desiredValue === undefined) {
      return [];
    }

    const desiredString = String(desiredValue);
    const current = currentConfig[key];

    return current === desiredString
      ? []
      : [{ key, desired: desiredString, current }];
  });
}

export function parseDescribeOutput(output: string): ParsedTopicDescription {
  return {
    partitions: extractDescribeNumber(output, [
      /^\s*partitions\s+(\d+)\s*$/im,
      /\bpartition\s*count\b\s*[:=]?\s*(\d+)/i,
      /\bpartitioncount\b\s*[:=]?\s*(\d+)/i,
    ]),
    replicas: extractDescribeNumber(output, [
      /^\s*replicas\s+(\d+)\s*$/im,
      /\breplication\s*factor\b\s*[:=]?\s*(\d+)/i,
      /\breplicationfactor\b\s*[:=]?\s*(\d+)/i,
    ]),
    config: extractDescribeConfig(output),
  };
}

async function waitForBroker(
  config: TopicManagerConfig,
  rpk: TopicAdminClient,
  logger: TopicManagerLogger,
): Promise<void> {
  logger.log("Connecting to Redpanda...");

  for (let attempt = 1; attempt <= config.waitAttempts; attempt += 1) {
    const result = await rpk.clusterInfo();
    if (commandSucceeded(result)) {
      logger.log("Broker available.");
      return;
    }

    logger.log(
      `Waiting for Redpanda (${attempt}/${config.waitAttempts}): ${formatCommandError(result)}`,
    );
    await sleep(config.waitSleepSeconds * 1000);
  }

  throw new Error("Timed out waiting for Redpanda broker metadata.");
}

function detectImmutableDrift(
  desired: KafkaTopicCatalogEntry,
  current: ParsedTopicDescription,
): string[] {
  const warnings: string[] = [];

  if (current.partitions !== undefined && current.partitions !== desired.partitions) {
    warnings.push(
      `Immutable drift: ${desired.topic} partitions desired=${desired.partitions} current=${current.partitions}`,
    );
  }

  if (current.replicas !== undefined && current.replicas !== desired.replicas) {
    warnings.push(
      `Immutable drift: ${desired.topic} replicas desired=${desired.replicas} current=${current.replicas}`,
    );
  }

  return warnings;
}

function extractDescribeConfig(output: string): Record<string, string> {
  const config: Record<string, string> = {};

  for (const key of KafkaTopicMutableConfigKeys) {
    const escapedKey = escapeRegExp(key);
    const patterns = [
      new RegExp(`\\b${escapedKey}\\b\\s*[=:]\\s*([^,\\s]+)`, "i"),
      new RegExp(`^\\s*${escapedKey}\\s+([^\\s]+)`, "im"),
    ];

    for (const pattern of patterns) {
      const match = output.match(pattern);
      if (match?.[1]) {
        config[key] = match[1].trim();
        break;
      }
    }
  }

  return config;
}

function extractDescribeNumber(
  output: string,
  patterns: RegExp[],
): number | undefined {
  for (const pattern of patterns) {
    const match = output.match(pattern);
    if (match?.[1]) {
      return Number(match[1]);
    }
  }

  return undefined;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function formatCommandError(result: CommandResult): string {
  return (result.stderr || result.stdout || `exit code ${result.code}`).trim();
}

function printSummary(summary: TopicManagerSummary, logger: TopicManagerLogger): void {
  logger.log("");
  logger.log("Kafka topic reconciliation summary");
  logger.log(`Topics total .......... ${summary.total}`);
  logger.log(`Created ............... ${summary.created}`);
  logger.log(`Updated ............... ${summary.updated}`);
  logger.log(`Skipped ............... ${summary.skipped}`);
  logger.log(`Warnings .............. ${summary.warnings}`);
  logger.log(`Errors ................ ${summary.errors}`);
}

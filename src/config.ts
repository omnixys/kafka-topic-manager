import { readFile } from "node:fs/promises";

export interface TopicManagerConfig {
  brokers: string[];
  rpkConfigOptions: string[];
  mutableConfig: boolean;
  dryRun: boolean;
  waitAttempts: number;
  waitSleepSeconds: number;
}

export const defaultConfig: TopicManagerConfig = {
  brokers: ["kafka.omnixys-data.svc.cluster.local:9092"],
  rpkConfigOptions: [],
  mutableConfig: true,
  dryRun: false,
  waitAttempts: 60,
  waitSleepSeconds: 5,
};

export async function loadConfig(path?: string): Promise<TopicManagerConfig> {
  const fromFile = path
    ? (JSON.parse(await readFile(path, "utf8")) as Partial<TopicManagerConfig>)
    : {};

  const brokersFromEnv = process.env.REDPANDA_BROKERS
    ?.split(",")
    .map((broker) => broker.trim())
    .filter(Boolean);

  return {
    ...defaultConfig,
    ...fromFile,
    brokers: brokersFromEnv?.length ? brokersFromEnv : fromFile.brokers ?? defaultConfig.brokers,
    rpkConfigOptions: fromFile.rpkConfigOptions ?? defaultConfig.rpkConfigOptions,
    mutableConfig: fromFile.mutableConfig ?? defaultConfig.mutableConfig,
    dryRun: fromFile.dryRun ?? defaultConfig.dryRun,
    waitAttempts: fromFile.waitAttempts ?? defaultConfig.waitAttempts,
    waitSleepSeconds: fromFile.waitSleepSeconds ?? defaultConfig.waitSleepSeconds,
  };
}

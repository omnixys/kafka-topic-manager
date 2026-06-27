import { spawn } from "node:child_process";

export interface CommandResult {
  code: number;
  stdout: string;
  stderr: string;
}

export interface CommandRunner {
  run(command: string, args: string[]): Promise<CommandResult>;
}

export class SpawnCommandRunner implements CommandRunner {
  async run(command: string, args: string[]): Promise<CommandResult> {
    return new Promise((resolve, reject) => {
      const child = spawn(command, args, {
        stdio: ["ignore", "pipe", "pipe"],
      });
      const stdout: Buffer[] = [];
      const stderr: Buffer[] = [];

      child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
      child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
      child.on("error", reject);
      child.on("close", (code) =>
        resolve({
          code: code ?? 1,
          stdout: Buffer.concat(stdout).toString("utf8"),
          stderr: Buffer.concat(stderr).toString("utf8"),
        }),
      );
    });
  }
}

export class RpkClient {
  constructor(
    private readonly brokers: string[],
    private readonly extraConfigOptions: string[] = [],
    private readonly runner: CommandRunner = new SpawnCommandRunner(),
  ) {}

  async clusterInfo(): Promise<CommandResult> {
    return this.run(["cluster", "info"]);
  }

  async describeTopic(topic: string): Promise<CommandResult> {
    return this.run(["topic", "describe", topic]);
  }

  async createTopic(input: {
    topic: string;
    partitions: number;
    replicas: number;
    config: Record<string, string | number | boolean>;
    dryRun?: boolean;
  }): Promise<CommandResult> {
    const args = [
      "topic",
      "create",
      input.topic,
      "--if-not-exists",
      "--partitions",
      String(input.partitions),
      "--replicas",
      String(input.replicas),
    ];

    if (input.dryRun) {
      args.push("--dry");
    }

    for (const [key, value] of Object.entries(input.config).sort()) {
      args.push("--topic-config", `${key}=${String(value)}`);
    }

    return this.run(args);
  }

  async alterTopicConfig(
    topic: string,
    config: Record<string, string | number | boolean>,
    dryRun = false,
  ): Promise<CommandResult> {
    const args = ["topic", "alter-config", topic];

    if (dryRun) {
      args.push("--dry");
    }

    for (const [key, value] of Object.entries(config).sort()) {
      args.push("--set", `${key}=${String(value)}`);
    }

    return this.run(args);
  }

  private async run(args: string[]): Promise<CommandResult> {
    const configArgs = [
      "-X",
      `brokers=${this.brokers.join(",")}`,
      ...this.extraConfigOptions.flatMap((option) => ["-X", option]),
    ];

    return this.runner.run("rpk", [...configArgs, ...args]);
  }
}

export function commandSucceeded(result: CommandResult): boolean {
  return result.code === 0;
}

export function topicMissing(result: CommandResult): boolean {
  const output = `${result.stdout}\n${result.stderr}`.toLowerCase();

  return (
    result.code !== 0 &&
    (output.includes("unknown topic") ||
      output.includes("does not exist") ||
      output.includes("not found") ||
      output.includes("unknown_topic_or_partition"))
  );
}

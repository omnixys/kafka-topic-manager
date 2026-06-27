import { KafkaTopics } from "@omnixys/kafka";
import assert from "node:assert/strict";
import test from "node:test";
import {
  RpkClient,
  detectMutableConfigDrift,
  parseDescribeOutput,
  reconcileTopics,
} from "../dist/index.js";

test("detectMutableConfigDrift reports desired mutable differences", () => {
  const drift = detectMutableConfigDrift(
    {
      topic: "notification.retry.whatsapp",
      domain: "whatsapp",
      key: "retry",
      owner: "notification",
      description: "retry",
      version: 1,
      producers: [],
      consumers: [],
      policy: "retry",
      partitions: 1,
      replicas: 1,
      config: {
        "cleanup.policy": "delete",
        "retention.ms": "86400000",
      },
    },
    {
      "cleanup.policy": "delete",
      "retention.ms": "604800000",
    },
  );

  assert.deepEqual(drift, [
    {
      key: "retention.ms",
      desired: "86400000",
      current: "604800000",
    },
  ]);
});

test("parseDescribeOutput extracts rpk text output", () => {
  const parsed = parseDescribeOutput(
    `
SUMMARY
=======
NAME        notification.retry.whatsapp
PARTITIONS  3
REPLICAS    1

CONFIGS
=======
KEY             VALUE
cleanup.policy  delete
retention.ms    604800000
`,
  );

  assert.equal(parsed.partitions, 3);
  assert.equal(parsed.replicas, 1);
  assert.equal(parsed.config["cleanup.policy"], "delete");
  assert.equal(parsed.config["retention.ms"], "604800000");
});

test("reconcileTopics creates missing topics and skips existing topics", async () => {
  const calls = [];
  const rpk = {
    async clusterInfo() {
      calls.push(["clusterInfo"]);
      return ok("{}");
    },
    async describeTopic(topic) {
      calls.push(["describeTopic", topic]);
      if (topic === KafkaTopics.user.createUser) {
        return missing();
      }
      return ok("");
    },
    async createTopic(input) {
      calls.push(["createTopic", input.topic]);
      return ok("");
    },
    async alterTopicConfig(topic, config) {
      calls.push(["alterTopicConfig", topic, config]);
      return ok("");
    },
  };

  const summary = await reconcileTopics(
    {
      brokers: ["localhost:9092"],
      rpkConfigOptions: [],
      mutableConfig: false,
      dryRun: false,
      waitAttempts: 1,
      waitSleepSeconds: 0,
    },
    rpk,
    silentLogger,
  );

  assert.equal(summary.created, 1);
  assert.equal(calls.some((call) => call[0] === "createTopic"), true);
});

test("reconcileTopics treats non-missing describe failures as errors", async () => {
  const calls = [];
  const rpk = {
    async clusterInfo() {
      calls.push(["clusterInfo"]);
      return ok("");
    },
    async describeTopic(topic) {
      calls.push(["describeTopic", topic]);
      return { code: 2, stdout: "", stderr: "authorization failed" };
    },
    async createTopic(input) {
      calls.push(["createTopic", input.topic]);
      return ok("");
    },
    async alterTopicConfig(topic, config) {
      calls.push(["alterTopicConfig", topic, config]);
      return ok("");
    },
  };

  await assert.rejects(
    reconcileTopics(
      {
        brokers: ["localhost:9092"],
        rpkConfigOptions: [],
        mutableConfig: false,
        dryRun: false,
        waitAttempts: 1,
        waitSleepSeconds: 0,
      },
      rpk,
      silentLogger,
    ),
    /finished with errors/,
  );

  assert.equal(calls.some((call) => call[0] === "createTopic"), false);
});

test("RpkClient describes topics without format flags", async () => {
  const calls = [];
  const runner = {
    async run(command, args) {
      calls.push([command, args]);
      return ok("");
    },
  };
  const client = new RpkClient(
    ["localhost:9092"],
    ["tls.enabled=true"],
    runner,
  );

  await client.describeTopic("notification.retry.whatsapp");

  assert.deepEqual(calls, [
    [
      "rpk",
      [
        "-X",
        "brokers=localhost:9092",
        "-X",
        "tls.enabled=true",
        "topic",
        "describe",
        "notification.retry.whatsapp",
      ],
    ],
  ]);
});

test("RpkClient creates topics with rpk v24.1.6 supported flags", async () => {
  const calls = [];
  const runner = {
    async run(command, args) {
      calls.push([command, args]);
      return ok("");
    },
  };
  const client = new RpkClient(["localhost:9092"], [], runner);

  await client.createTopic({
    topic: "notification.retry.whatsapp",
    partitions: 3,
    replicas: 1,
    config: {
      "retention.ms": "604800000",
      "cleanup.policy": "delete",
    },
    dryRun: true,
  });

  assert.deepEqual(calls, [
    [
      "rpk",
      [
        "-X",
        "brokers=localhost:9092",
        "topic",
        "create",
        "notification.retry.whatsapp",
        "--partitions",
        "3",
        "--replicas",
        "1",
        "--dry",
        "--topic-config",
        "cleanup.policy=delete",
        "--topic-config",
        "retention.ms=604800000",
      ],
    ],
  ]);
});

function ok(stdout) {
  return { code: 0, stdout, stderr: "" };
}

function missing() {
  return { code: 1, stdout: "", stderr: "unknown topic or partition" };
}

const silentLogger = {
  log() {},
  warn() {},
  error() {},
};

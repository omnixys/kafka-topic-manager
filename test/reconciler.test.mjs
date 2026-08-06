import { KafkaTopics } from "@omnixys/kafka-ts";
import assert from "node:assert/strict";
import test from "node:test";
import {
  RpkClient,
  detectMutableConfigDrift,
  parseDescribeOutput,
  parseTopicList,
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

test("parseTopicList extracts topic names from rpk table output", () => {
  assert.deepEqual(
    [...parseTopicList("NAME  PARTITIONS  REPLICAS\nfoo  1  1\nbar.retry  3  1\n")],
    ["foo", "bar.retry"],
  );
});

test("reconcileTopics batches the expanded catalog by topic configuration", async () => {
  const batches = [];
  const rpk = {
    async clusterInfo() {
      return ok("{}");
    },
    async listTopics() {
      return ok("NAME  PARTITIONS  REPLICAS\n");
    },
    async describeTopic() {
      throw new Error("missing topics must not be described");
    },
    async createTopic() {
      throw new Error("batch-capable clients must not create topics serially");
    },
    async createTopics(input) {
      batches.push(input);
      return ok("");
    },
    async alterTopicConfig() {
      return ok("");
    },
  };

  const summary = await reconcileTopics(
    {
      brokers: ["localhost:9092"],
      rpkConfigOptions: [],
      mutableConfig: true,
      dryRun: false,
      waitAttempts: 1,
      waitSleepSeconds: 0,
    },
    rpk,
    silentLogger,
  );

  assert.equal(summary.total, 461);
  assert.equal(summary.created, 461);
  assert.equal(batches.length, 5);
  assert.deepEqual(
    batches.map((batch) => batch.topics.length).sort((a, b) => a - b),
    [2, 2, 149, 154, 154],
  );
  assert.equal(
    batches.every((batch) =>
      batch.topics.every(
        (topic, index) => index === 0 || batch.topics[index - 1] < topic,
      ),
    ),
    true,
  );
});

test("reconcileTopics describes existing topics and batches only missing topics", async () => {
  const existingTopic = KafkaTopics.user.createUser;
  const described = [];
  const batches = [];
  const rpk = {
    async clusterInfo() {
      return ok("{}");
    },
    async listTopics() {
      return ok(`NAME  PARTITIONS  REPLICAS\n${existingTopic}  1  1\n`);
    },
    async describeTopic(topic) {
      described.push(topic);
      return ok("");
    },
    async createTopic() {
      throw new Error("batch-capable clients must not create topics serially");
    },
    async createTopics(input) {
      batches.push(input);
      return ok("");
    },
    async alterTopicConfig() {
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

  assert.deepEqual(described, [existingTopic]);
  assert.equal(summary.created, 460);
  assert.equal(
    batches.flatMap((batch) => batch.topics).includes(existingTopic),
    false,
  );
});

test("reconcileTopics fails when a topic batch cannot be created", async () => {
  const rpk = {
    async clusterInfo() {
      return ok("{}");
    },
    async listTopics() {
      return ok("NAME  PARTITIONS  REPLICAS\n");
    },
    async describeTopic() {
      throw new Error("missing topics must not be described");
    },
    async createTopic() {
      throw new Error("batch-capable clients must not create topics serially");
    },
    async createTopics() {
      return { code: 1, stdout: "", stderr: "batch rejected" };
    },
    async alterTopicConfig() {
      return ok("");
    },
  };

  await assert.rejects(
    reconcileTopics(
      {
        brokers: ["localhost:9092"],
        rpkConfigOptions: [],
        mutableConfig: true,
        dryRun: false,
        waitAttempts: 1,
        waitSleepSeconds: 0,
      },
      rpk,
      silentLogger,
    ),
    /finished with errors/,
  );
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

test("reconcileTopics creates derived retry and DLQ topics from the expanded catalog", async () => {
  const missingTopics = new Set([
    `${KafkaTopics.user.createUser}.retry`,
    `${KafkaTopics.user.createUser}.dlq`,
  ]);
  const created = [];
  const rpk = {
    async clusterInfo() {
      return ok("{}");
    },
    async describeTopic(topic) {
      return missingTopics.has(topic) ? missing() : ok("");
    },
    async createTopic(input) {
      created.push(input);
      return ok("");
    },
    async alterTopicConfig() {
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

  const retry = created.find(
    (entry) => entry.topic === `${KafkaTopics.user.createUser}.retry`,
  );
  const dlq = created.find(
    (entry) => entry.topic === `${KafkaTopics.user.createUser}.dlq`,
  );

  assert.equal(summary.created, 2);
  assert.equal(retry?.config["retention.ms"], "86400000");
  assert.equal(dlq?.config["retention.ms"], "2592000000");
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

test("RpkClient lists topics and creates deterministic topic batches", async () => {
  const calls = [];
  const runner = {
    async run(command, args) {
      calls.push([command, args]);
      return ok("");
    },
  };
  const client = new RpkClient(["localhost:9092"], [], runner);

  await client.listTopics();
  await client.createTopics({
    topics: ["bar.retry", "foo.retry"],
    partitions: 1,
    replicas: 1,
    config: {
      "retention.ms": "86400000",
      "cleanup.policy": "delete",
    },
    dryRun: true,
  });

  assert.deepEqual(calls, [
    [
      "rpk",
      ["-X", "brokers=localhost:9092", "topic", "list"],
    ],
    [
      "rpk",
      [
        "-X",
        "brokers=localhost:9092",
        "topic",
        "create",
        "bar.retry",
        "foo.retry",
        "--partitions",
        "1",
        "--replicas",
        "1",
        "--dry",
        "--topic-config",
        "cleanup.policy=delete",
        "--topic-config",
        "retention.ms=86400000",
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

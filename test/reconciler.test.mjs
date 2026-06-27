import { KafkaTopics } from "@omnixys/kafka";
import assert from "node:assert/strict";
import test from "node:test";
import {
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

test("parseDescribeOutput extracts common config shapes", () => {
  const parsed = parseDescribeOutput(
    JSON.stringify({
      summary: { partitions: 3, replicas: 1 },
      configs: [
        { name: "cleanup.policy", value: "delete" },
        { name: "retention.ms", value: "604800000" },
      ],
    }),
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
    async listTopics() {
      calls.push(["listTopics"]);
      return ok("[]");
    },
    async describeTopic(topic) {
      calls.push(["describeTopic", topic]);
      if (topic === KafkaTopics.user.createUser) {
        return missing();
      }
      return ok(JSON.stringify({ configs: [] }));
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

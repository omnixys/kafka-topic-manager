#!/usr/bin/env node
import { loadConfig } from "./config.js";
import { reconcileTopics } from "./reconciler.js";

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const command = args[0] ?? "reconcile";

  if (command !== "reconcile") {
    throw new Error(`Unsupported command '${command}'. Use 'reconcile'.`);
  }

  const configPath = getFlagValue(args, "--config");
  const config = await loadConfig(configPath);
  await reconcileTopics(config);
}

function getFlagValue(args: string[], flag: string): string | undefined {
  const index = args.indexOf(flag);
  return index >= 0 ? args[index + 1] : undefined;
}

main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(message);
  process.exitCode = 1;
});

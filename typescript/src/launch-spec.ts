import { createHash } from "node:crypto";

import { stableCheckpointJsonBytes } from "./checkpoint.js";

const REPO_ID = /^[A-Za-z0-9][A-Za-z0-9._-]*\/[A-Za-z0-9][A-Za-z0-9._-]*$/u;

export type LaunchSpec = {
  schema_version: 1;
  image: string;
  command: readonly string[];
  flavor: string;
  timeout: string;
  environment: Readonly<Record<string, string>>;
  secret_names: readonly string[];
  labels: Readonly<Record<string, string>>;
  namespace?: string;
};

export function parseLaunchSpec(value: unknown): LaunchSpec {
  const record = requireRecord(value, "launch specification");
  requireExactKeys(
    record,
    [
      "schema_version",
      "image",
      "command",
      "flavor",
      "timeout",
      "environment",
      "secret_names",
      "labels",
    ],
    ["namespace"],
  );
  if (record.schema_version !== 1) throw new Error("schema_version must be 1");
  const command = requireStringArray(record.command, "command");
  const secretNames = requireStringArray(record.secret_names, "secret_names");
  const environment = requireStringRecord(record.environment, "environment");
  const labels = requireStringRecord(record.labels, "labels");
  const reserved = ["RUN_ID", "ATTEMPT_ID", "PLAN_SHA256"];
  if (reserved.some((name) => name in environment)) {
    throw new Error(
      "RUN_ID, ATTEMPT_ID, and PLAN_SHA256 are assigned by the launcher",
    );
  }
  if (new Set(secretNames).size !== secretNames.length) {
    throw new Error("secret_names must be unique");
  }
  const namespace =
    record.namespace === undefined
      ? undefined
      : requireRepoNamespace(record.namespace, "namespace");
  return {
    schema_version: 1,
    image: requireBoundedString(record.image, "image", 500),
    command,
    flavor: requireBoundedString(record.flavor, "flavor", 100),
    timeout: requireBoundedString(record.timeout, "timeout", 100),
    environment,
    secret_names: secretNames,
    labels,
    ...(namespace === undefined ? {} : { namespace }),
  };
}

export function launchSpecSha256(spec: LaunchSpec): string {
  return createHash("sha256")
    .update(stableCheckpointJsonBytes(parseLaunchSpec(spec)))
    .digest("hex");
}

function requireRecord(value: unknown, name: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${name} must be an object`);
  }
  const entries = Object.entries(value);
  if (entries.some(([key]) => typeof key !== "string")) {
    throw new Error(`${name} keys must be strings`);
  }
  return Object.fromEntries(entries);
}

function requireExactKeys(
  record: Record<string, unknown>,
  required: readonly string[],
  optional: readonly string[],
): void {
  const allowed = new Set([...required, ...optional]);
  const missing = required.filter((key) => !(key in record));
  const unexpected = Object.keys(record).filter((key) => !allowed.has(key));
  if (missing.length > 0)
    throw new Error(`missing fields: ${missing.sort().join(", ")}`);
  if (unexpected.length > 0) {
    throw new Error(`unexpected fields: ${unexpected.sort().join(", ")}`);
  }
}

function requireBoundedString(
  value: unknown,
  name: string,
  maximum: number,
): string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > maximum
  ) {
    throw new Error(`${name} must contain 1 to ${String(maximum)} characters`);
  }
  return value;
}

function requireStringArray(value: unknown, name: string): readonly string[] {
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
    throw new Error(`${name} must be an array of strings`);
  }
  return value.map((item) => String(item));
}

function requireStringRecord(
  value: unknown,
  name: string,
): Readonly<Record<string, string>> {
  const record = requireRecord(value, name);
  if (Object.values(record).some((item) => typeof item !== "string")) {
    throw new Error(`${name} values must be strings`);
  }
  return Object.fromEntries(
    Object.entries(record).map(([key, item]) => [key, String(item)]),
  );
}

function requireRepoNamespace(value: unknown, name: string): string {
  const result = requireBoundedString(value, name, 200);
  if (result.includes("/") || !REPO_ID.test(`${result}/repo`)) {
    throw new Error(`${name} must be a safe Hub namespace`);
  }
  return result;
}

import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { test } from "node:test";

import {
  launchSpecSha256,
  parseLaunchSpec,
  stableCheckpointJsonBytes,
} from "../src/index.js";

test("launch specification fixture has Python parity", async () => {
  const raw = await readFile("fixtures/launch-spec-v1.json");
  const spec = parseLaunchSpec(JSON.parse(raw.toString("utf8")));
  assert.deepEqual(stableCheckpointJsonBytes(spec), raw);
  assert.equal(
    launchSpecSha256(spec),
    createHash("sha256").update(raw).digest("hex"),
  );
});

test("launch specification rejects unknown fields and duplicate secrets", () => {
  const base = {
    schema_version: 1,
    image: "node:24",
    command: ["node", "worker.js"],
    flavor: "cpu-basic",
    timeout: "6h",
    environment: {},
    secret_names: ["HF_TOKEN"],
    labels: {},
  };
  assert.throws(
    () => parseLaunchSpec({ ...base, extra: true }),
    /unexpected fields/u,
  );
  assert.throws(
    () => parseLaunchSpec({ ...base, secret_names: ["HF_TOKEN", "HF_TOKEN"] }),
    /must be unique/u,
  );
});

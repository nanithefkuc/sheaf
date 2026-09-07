import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { test } from "node:test";

import { encodeFrame, FrameParser, resolveSocketPath, sha256 } from "../../ipc";

test("resolveSocketPath honors SHEAF_SOCKET over everything", () => {
  const resolved = resolveSocketPath({ SHEAF_SOCKET: "/run/custom.sock" } as NodeJS.ProcessEnv);
  assert.equal(resolved, "/run/custom.sock");
});

test("resolveSocketPath uses XDG_RUNTIME_DIR when no explicit socket", () => {
  const resolved = resolveSocketPath({ XDG_RUNTIME_DIR: "/run/user/1000" } as NodeJS.ProcessEnv);
  assert.equal(resolved, "/run/user/1000/sheaf/control.sock");
});

test("resolveSocketPath falls back to the per-uid tmp socket", () => {
  const resolved = resolveSocketPath({} as NodeJS.ProcessEnv);
  assert.match(resolved, /^\/tmp\/sheaf-\d+\/sheaf\/control\.sock$/);
});

test("FrameParser reassembles frames split across arbitrary chunk boundaries", () => {
  const parser = new FrameParser();
  const wire = Buffer.concat([
    encodeFrame(Buffer.from("alpha")),
    encodeFrame(Buffer.from("")),
    encodeFrame(Buffer.from("omega")),
  ]);
  const seen: string[] = [];
  for (const byte of wire) {
    parser.push(Buffer.from([byte]));
    for (;;) {
      const frame = parser.next(1024);
      if (frame === undefined) {
        break;
      }
      seen.push(frame.toString("utf8"));
    }
  }
  assert.deepEqual(seen, ["alpha", "", "omega"]);
});

test("FrameParser refuses a frame past the cap", () => {
  const parser = new FrameParser();
  parser.push(encodeFrame(Buffer.alloc(64)));
  assert.throws(() => parser.next(16), /exceeds cap/);
});

test("sha256 matches the Rust store digest of the same bytes", () => {
  const expected = createHash("sha256").update("one two\n", "utf8").digest("hex");
  assert.equal(sha256("one two\n"), expected);
  assert.equal(sha256("one two\n").length, 64);
});

import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { test } from "node:test";

import {
  encodeFrame,
  FrameParser,
  IpcConnection,
  IpcError,
  MAX_CHUNK,
  RequestEnvelope,
  SheafClient,
} from "../../ipc";

/** Answer one `ping` with the given proto and capability catalog. */
function servePing(
  proto: { major: number; minor: number },
  capabilities: string[],
): Promise<string> {
  return serveOnce(({ request, socket }) => {
    assert.equal(request.method, "ping");
    const envelope = {
      v: 1,
      id: request.id,
      ok: true,
      result: { proto, daemon_version: "0.1.0", capabilities },
    };
    socket.write(encodeFrame(Buffer.from(JSON.stringify(envelope), "utf8")));
  });
}

interface Served {
  request: RequestEnvelope;
  body: Buffer;
  socket: net.Socket;
}

/** A one-shot Unix-socket server that decodes one request (with body). */
function serveOnce(handler: (served: Served) => void): Promise<string> {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "sheaf-ipc-"));
  const socketPath = path.join(dir, "control.sock");
  const parser = new FrameParser();
  const server = net.createServer((socket) => {
    let request: RequestEnvelope | undefined;
    let bodyRemaining = 0;
    const body: Buffer[] = [];
    socket.on("data", (chunk: Buffer) => {
      parser.push(chunk);
      for (;;) {
        if (request === undefined) {
          const frame = parser.next(1024 * 1024);
          if (frame === undefined) {
            return;
          }
          request = JSON.parse(frame.toString("utf8")) as RequestEnvelope;
          bodyRemaining = request.body ? request.body.chunks : 0;
        }
        if (bodyRemaining > 0) {
          const frame = parser.next(MAX_CHUNK);
          if (frame === undefined) {
            return;
          }
          body.push(frame);
          bodyRemaining -= 1;
          continue;
        }
        handler({ request, body: Buffer.concat(body), socket });
        server.close();
        return;
      }
    });
  });
  server.unref();
  return new Promise((resolve) => server.listen(socketPath, () => resolve(socketPath)));
}

test("call uploads a counted body and reassembles a counted response body", async () => {
  const upload = Buffer.alloc(MAX_CHUNK + 7, 7);
  const socketPath = await serveOnce(({ request, body, socket }) => {
    assert.equal(request.method, "editor.capture");
    assert.equal(request.body?.chunks, 2);
    assert.equal(body.length, upload.length);
    const envelope = {
      v: 1,
      id: request.id,
      ok: true,
      result: { recorded: true },
      body: { chunks: 1 },
    };
    socket.write(encodeFrame(Buffer.from(JSON.stringify(envelope), "utf8")));
    socket.write(encodeFrame(Buffer.from("one two\n", "utf8")));
  });
  const connection = await IpcConnection.connect(socketPath);
  const reply = await connection.call("editor.capture", "/proj", { kind: "edit" }, upload);
  assert.equal(reply.response.ok, true);
  assert.equal(reply.body.toString("utf8"), "one two\n");
  connection.dispose();
});

test("a dropped connection rejects the in-flight call", async () => {
  const socketPath = await serveOnce(({ socket }) => socket.destroy());
  const connection = await IpcConnection.connect(socketPath);
  await assert.rejects(connection.call("ping", undefined, null));
  connection.dispose();
});

test("a stale daemon proto is reported as a version mismatch, not opaque caps", async () => {
  // Minor 13 predates the editor protocol (minor 14); the gate must name the
  // version gap and the fix so a semver delta is what surfaces in the log.
  const socketPath = await servePing({ major: 1, minor: 13 }, ["timeline.log", "diff"]);
  const client = new SheafClient(socketPath, () => {});
  await assert.rejects(client.ping(), (error: unknown) => {
    assert.ok(error instanceof IpcError);
    assert.equal(error.code, "editor.unsupported");
    assert.match(error.message, /proto 1\.13/);
    assert.match(error.message, /proto 1\.14\+/);
    assert.match(error.message, /predates the editor protocol/);
    assert.match(error.message, /project\.resolve, editor\.capture, editor\.step/);
    return true;
  });
  client.dispose();
});

test("a new-enough daemon missing caps is reported as an unexpected build", async () => {
  // Proto is current but the catalog still lacks a method: not the ordinary
  // upgrade case, so the message must not blame the version.
  const socketPath = await servePing(
    { major: 1, minor: 14 },
    ["project.resolve", "editor.capture"],
  );
  const client = new SheafClient(socketPath, () => {});
  await assert.rejects(client.ping(), (error: unknown) => {
    assert.ok(error instanceof IpcError);
    assert.equal(error.code, "editor.unsupported");
    assert.match(error.message, /new enough/);
    assert.match(error.message, /editor\.step/);
    assert.doesNotMatch(error.message, /predates/);
    return true;
  });
  client.dispose();
});

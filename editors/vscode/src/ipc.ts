import * as net from "node:net";
import { createHash } from "node:crypto";
import * as path from "node:path";

export const PROTO_MAJOR = 1;
/**
 * Minimum proto minor this extension needs from the daemon: minor 14 is where
 * `project.resolve`, `editor.capture`, and `editor.step` joined the catalog.
 * A daemon below it is simply too old, and the capability gate reports that as
 * a version mismatch rather than an opaque missing-capability list.
 */
export const PROTO_MINOR = 14;
export const MAX_ENVELOPE = 1024 * 1024;
export const MAX_CHUNK = 256 * 1024;

/** Lowercase SHA-256 hex of a UTF-8 string; mirrors `store::hash_of`. */
export function sha256(text: string): string {
  return createHash("sha256").update(text, "utf8").digest("hex");
}

/** Structured daemon error: a stable machine code plus a human message. */
export class IpcError extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "IpcError";
  }
}

export interface RequestEnvelope {
  v: number;
  id: string;
  method: string;
  project?: string;
  params: unknown;
  body?: { chunks: number };
}

export interface ResponseEnvelope {
  v: number;
  id: string;
  ok: boolean;
  result?: unknown;
  error?: { code: string; message: string };
  body?: { chunks: number };
}

export interface Reply {
  response: ResponseEnvelope;
  body: Buffer;
}

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (error: Error) => void;
}

/**
 * A settled-later promise with externally callable resolvers. The extension
 * host runs on the Electron Node (18) which lacks `Promise.withResolvers`, so
 * this is the single sanctioned executor use the rest of the module builds on.
 */
function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/** Resolve the control socket exactly like `paths::control_socket_path`. */
export function resolveSocketPath(env: NodeJS.ProcessEnv = process.env): string {
  const explicit = env.SHEAF_SOCKET;
  if (explicit && explicit.length > 0) {
    return explicit;
  }
  const runtime = env.XDG_RUNTIME_DIR;
  const getuid = (process as NodeJS.Process & { getuid?: () => number }).getuid;
  const id = typeof getuid === "function" ? getuid.call(process) : 0;
  const base = runtime && runtime.length > 0 ? runtime : `/tmp/sheaf-${id}`;
  return path.join(base, "sheaf", "control.sock");
}

/** One length-prefixed frame: u32-LE length, then payload. */
export function encodeFrame(payload: Buffer): Buffer {
  const header = Buffer.allocUnsafe(4);
  header.writeUInt32LE(payload.length, 0);
  return Buffer.concat([header, payload]);
}

/**
 * Incremental length-prefixed frame reader. Feed bytes as they arrive; each
 * `next()` yields one complete frame payload, or `undefined` when more bytes
 * are needed.
 */
export class FrameParser {
  private buffer: Buffer = Buffer.alloc(0);

  push(chunk: Buffer): void {
    this.buffer = this.buffer.length === 0 ? chunk : Buffer.concat([this.buffer, chunk]);
  }

  next(cap: number): Buffer | undefined {
    if (this.buffer.length < 4) {
      return undefined;
    }
    const length = this.buffer.readUInt32LE(0);
    if (length > cap) {
      throw new IpcError("internal", `frame length ${length} exceeds cap ${cap}`);
    }
    if (this.buffer.length < 4 + length) {
      return undefined;
    }
    const payload = Buffer.from(this.buffer.subarray(4, 4 + length));
    this.buffer = Buffer.from(this.buffer.subarray(4 + length));
    return payload;
  }
}

interface Pending {
  resolve: (reply: Reply) => void;
  reject: (error: Error) => void;
  bodyRemaining: number;
  response?: ResponseEnvelope;
  bodyChunks: Buffer[];
}

/**
 * One shared Unix-socket connection. Requests are framed out and their
 * responses (with optional counted bodies) reassembled in order. A single
 * request is in flight at a time; callers await sequentially.
 */
export class IpcConnection {
  private parser = new FrameParser();
  private pending: Pending | undefined;
  private queue: Array<() => void> = [];
  private closed = false;
  private nextId = 1;

  private constructor(private socket: net.Socket) {
    socket.on("data", (chunk: Buffer) => this.onData(chunk));
    socket.on("error", (error: Error) => this.fail(error));
    socket.on("close", () => this.fail(new IpcError("internal", "connection closed")));
  }

  static connect(socketPath: string, timeoutMs = 5000): Promise<IpcConnection> {
    const gate = deferred<IpcConnection>();
    const socket = net.createConnection(socketPath);
    const onError = (error: Error) => {
      socket.destroy();
      gate.reject(new IpcError("internal", `connect ${socketPath}: ${error.message}`));
    };
    const timer = setTimeout(() => onError(new Error("connect timed out")), timeoutMs);
    socket.once("error", onError);
    socket.once("connect", () => {
      clearTimeout(timer);
      socket.removeListener("error", onError);
      gate.resolve(new IpcConnection(socket));
    });
    return gate.promise;
  }

  isClosed(): boolean {
    return this.closed;
  }

  dispose(): void {
    this.closed = true;
    this.socket.destroy();
  }

  /** Send one request and await its full reply. Serialized per connection. */
  async call(
    method: string,
    project: string | undefined,
    params: unknown,
    body?: Buffer,
  ): Promise<Reply> {
    if (this.closed) {
      throw new IpcError("internal", "connection is closed");
    }
    if (this.pending !== undefined || this.queue.length > 0) {
      const slot = deferred<void>();
      this.queue.push(slot.resolve);
      await slot.promise;
    }
    try {
      return await this.send(method, project, params, body);
    } finally {
      const next = this.queue.shift();
      if (next) {
        next();
      }
    }
  }

  private send(
    method: string,
    project: string | undefined,
    params: unknown,
    body?: Buffer,
  ): Promise<Reply> {
    const gate = deferred<Reply>();
    const id = String(this.nextId++);
    const envelope: RequestEnvelope = { v: PROTO_MAJOR, id, method, params };
    if (project !== undefined) {
      envelope.project = project;
    }
    if (body !== undefined) {
      if (body.length > MAX_ENVELOPE) {
        gate.reject(new IpcError("bad.request", `request body ${body.length} over cap`));
        return gate.promise;
      }
      envelope.body = { chunks: Math.ceil(body.length / MAX_CHUNK) };
    }
    this.pending = { resolve: gate.resolve, reject: gate.reject, bodyRemaining: 0, bodyChunks: [] };
    try {
      this.socket.write(encodeFrame(Buffer.from(JSON.stringify(envelope), "utf8")));
      if (body !== undefined) {
        for (let offset = 0; offset < body.length; offset += MAX_CHUNK) {
          this.socket.write(encodeFrame(body.subarray(offset, offset + MAX_CHUNK)));
        }
      }
    } catch (error) {
      this.pending = undefined;
      gate.reject(error instanceof Error ? error : new IpcError("internal", String(error)));
    }
    return gate.promise;
  }

  private onData(chunk: Buffer): void {
    this.parser.push(chunk);
    try {
      this.drain();
    } catch (error) {
      this.fail(error instanceof Error ? error : new IpcError("internal", String(error)));
    }
  }

  private drain(): void {
    for (;;) {
      const pending = this.pending;
      if (pending === undefined) {
        return;
      }
      if (pending.response === undefined) {
        const frame = this.parser.next(MAX_ENVELOPE);
        if (frame === undefined) {
          return;
        }
        const response = JSON.parse(frame.toString("utf8")) as ResponseEnvelope;
        pending.response = response;
        pending.bodyRemaining = response.body ? response.body.chunks : 0;
      }
      while (pending.bodyRemaining > 0) {
        const frame = this.parser.next(MAX_CHUNK);
        if (frame === undefined) {
          return;
        }
        pending.bodyChunks.push(frame);
        pending.bodyRemaining -= 1;
      }
      const response = pending.response;
      const body = Buffer.concat(pending.bodyChunks);
      this.pending = undefined;
      pending.resolve({ response, body });
    }
  }

  private fail(error: Error): void {
    this.closed = true;
    const pending = this.pending;
    this.pending = undefined;
    if (pending) {
      pending.reject(error instanceof IpcError ? error : new IpcError("internal", error.message));
    }
    for (const waiter of this.queue.splice(0)) {
      waiter();
    }
  }
}

export interface Eligibility {
  class: "durable" | "volatile";
  regular: boolean;
  symlink: boolean;
  bytes: number;
  supported: boolean;
}

export interface ProjectResolveReply {
  root: string;
  store_root: string;
  registered: boolean;
  watching: boolean;
  ready: boolean;
  cold: boolean;
  relative_path: string;
  eligibility: Eligibility;
  watch: { debounce_ms: number; max_hold_ms: number };
}

export interface EditorCaptureReply {
  recorded: boolean;
  physical_capture_id: string | null;
  cursor: string | null;
  content_sha256: string;
  rebased: boolean;
}

export type EditorStepReply =
  | {
      changed: true;
      source: string;
      target: string;
      content_sha256: string;
      bytes: number;
      text: string;
    }
  | { changed: false; reason: string };

export interface PingReply {
  major: number;
  minor: number;
  version: string;
  capabilities: string[];
}

const REQUIRED_CAPABILITIES = ["project.resolve", "editor.capture", "editor.step"];

/**
 * Explain why the editor capability gate failed. When the daemon's proto is
 * older than {@link PROTO_MAJOR}.{@link PROTO_MINOR} the missing capabilities
 * are a symptom of a stale `sheafd`, so lead with the concrete version gap and
 * the fix; a semver delta is far faster to act on than a bare capability list.
 * A new-enough daemon that still omits them is an unexpected build, reported as
 * such so it is not mistaken for the ordinary upgrade case.
 */
function capabilityGateMessage(ping: PingReply, missing: string[]): string {
  const daemon = `daemon proto ${ping.major}.${ping.minor} (sheaf ${ping.version})`;
  const want = `${PROTO_MAJOR}.${PROTO_MINOR}`;
  if (ping.major !== PROTO_MAJOR || ping.minor < PROTO_MINOR) {
    return (
      `${daemon} predates the editor protocol; this extension needs proto ${want}+. ` +
      `Rebuild and reinstall sheafd, then restart the daemon. ` +
      `(missing capabilities: ${missing.join(", ")})`
    );
  }
  return (
    `${daemon} is new enough (need proto ${want}+) but does not advertise: ` +
    `${missing.join(", ")}. Verify the sheafd build.`
  );
}

/**
 * The extension host's single Sheaf connection. Reconnects lazily on the next
 * call after a drop, verifies the three editor capabilities exactly once per
 * connection, and surfaces transport loss through `onDisconnect` so the caller
 * can clear its active state instead of blindly replaying an unknown request.
 */
export class SheafClient {
  private connection: IpcConnection | undefined;
  private connecting: Promise<IpcConnection> | undefined;

  constructor(
    private readonly socketPath: string,
    private readonly onDisconnect: () => void,
  ) {}

  dispose(): void {
    this.connection?.dispose();
    this.connection = undefined;
    this.connecting = undefined;
  }

  private async connect(): Promise<IpcConnection> {
    if (this.connection && !this.connection.isClosed()) {
      return this.connection;
    }
    if (this.connecting) {
      return this.connecting;
    }
    this.connecting = (async () => {
      const connection = await IpcConnection.connect(this.socketPath);
      const ping = await this.pingOn(connection);
      const missing = REQUIRED_CAPABILITIES.filter((cap) => !ping.capabilities.includes(cap));
      if (missing.length > 0) {
        connection.dispose();
        throw new IpcError("editor.unsupported", capabilityGateMessage(ping, missing));
      }
      this.connection = connection;
      return connection;
    })();
    try {
      return await this.connecting;
    } finally {
      this.connecting = undefined;
    }
  }

  private async pingOn(connection: IpcConnection): Promise<PingReply> {
    const reply = await connection.call("ping", undefined, null);
    if (!reply.response.ok) {
      throw errorOf(reply.response);
    }
    const result = reply.response.result as {
      proto?: { major?: number; minor?: number };
      daemon_version?: string;
      capabilities?: string[];
    };
    return {
      major: result.proto?.major ?? 0,
      minor: result.proto?.minor ?? 0,
      version: result.daemon_version ?? "unknown",
      capabilities: result.capabilities ?? [],
    };
  }

  private async invoke(
    method: string,
    project: string | undefined,
    params: unknown,
    body?: Buffer,
  ): Promise<Reply> {
    let connection: IpcConnection;
    try {
      connection = await this.connect();
    } catch (error) {
      this.onDisconnect();
      throw error;
    }
    try {
      return await connection.call(method, project, params, body);
    } catch (error) {
      // Transport loss: drop the connection and let the caller decide, never
      // replay a request whose fate on the wire is unknown.
      this.connection = undefined;
      this.onDisconnect();
      throw error instanceof Error ? error : new IpcError("internal", String(error));
    }
  }

  async ping(): Promise<PingReply> {
    const connection = await this.connect();
    return this.pingOn(connection);
  }

  async resolve(absolutePath: string): Promise<ProjectResolveReply> {
    const reply = await this.invoke("project.resolve", undefined, { path: absolutePath });
    if (!reply.response.ok) {
      throw errorOf(reply.response);
    }
    return reply.response.result as ProjectResolveReply;
  }

  async editorCapture(
    project: string,
    params: {
      path: string;
      kind: "edit" | "undo" | "redo";
      base_sha256: string;
      saved_sha256: string | null;
      source: string | null;
      target: string | null;
    },
    text: string,
  ): Promise<EditorCaptureReply> {
    const reply = await this.invoke("editor.capture", project, params, Buffer.from(text, "utf8"));
    if (!reply.response.ok) {
      throw errorOf(reply.response);
    }
    return reply.response.result as EditorCaptureReply;
  }

  async editorStep(
    project: string,
    params: {
      path: string;
      cursor: string;
      direction: "undo" | "redo";
      target: string | null;
      current_sha256: string;
    },
  ): Promise<EditorStepReply> {
    const reply = await this.invoke("editor.step", project, params);
    if (!reply.response.ok) {
      throw errorOf(reply.response);
    }
    const result = reply.response.result as {
      changed: boolean;
      source?: string;
      target?: string;
      content_sha256?: string;
      bytes?: number;
      reason?: string;
    };
    if (!result.changed) {
      return { changed: false, reason: result.reason ?? "start_of_text_history" };
    }
    return {
      changed: true,
      source: result.source ?? "",
      target: result.target ?? "",
      content_sha256: result.content_sha256 ?? "",
      bytes: result.bytes ?? reply.body.length,
      text: reply.body.toString("utf8"),
    };
  }
}

function errorOf(response: ResponseEnvelope): IpcError {
  const error = response.error;
  return new IpcError(error?.code ?? "internal", error?.message ?? "unknown error");
}

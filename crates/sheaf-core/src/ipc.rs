//! Wire protocol client + frame codec: length-prefixed JSON envelopes
//! (≤1 MiB), raw byte continuation chunks (≤256 KiB, count pre-announced),
//! strict request→response correlation.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Result, SheafError};

/// Minor 1: `restore.plan` streams its plan through body chunks
/// with a summary in the envelope, and `restore.resume` / `restore.abandon`
/// / `store.gc` join the method catalog.
///
/// Minor 2: `timeline.grep` joins the catalog — a read-only literal
/// history search whose bounded summary rides the envelope while hit/event
/// records stream as NDJSON body chunks. Additive only.
///
/// Minor 3: `fragment.plan` / `fragment.apply` join the catalog —
/// selection-scoped restore over the same token discipline as whole-file
/// restore. Additive only.
///
/// Minor 4: `smart.plan` joins the catalog — selection-scoped
/// squash planning. Two phases over one method (candidate HEAD paths, then
/// the plan); the commit itself stays CLI-side git orchestration, because
/// squashing is opt-in and partial commits are kept off the anchor path.
/// Additive only.
///
/// Minor 5: `timeline.grep` results stream as the walk produces them; the
/// bounded summary rides the envelope while records flow as NDJSON chunks.
///
/// Minor 6: occurrence-centered grep — point/history modes,
/// coordinates, episode identity, and partial-capture record cursors.
/// Omitted mode remains legacy history.
///
/// Minor 7: occurrence anchors — the additive `anchor` request
/// field (coordinate/selection/episode) selects one followed episode. The
/// `timeline.grep.anchors` capability gates it because an older daemon
/// silently drops the unknown field and would answer unanchored.
///
/// Minor 9: live managed worktrees (`worktree.list` / `worktree.add`) and
/// explicit squash merge planning/application/resume join the catalog.
/// Additive only.

pub const PROTO_MAJOR: u32 = 1;
/// Minor 1.10: named branches, lifecycle operations, and branch metadata.
///
/// Minor 11: `timeline.log` accepts an optional `omit_paths` flag that
/// drops per-capture path lists from the reply. The squash span walk sets
/// it so a full page stays under the envelope cap even across bulk-change
/// captures; an older daemon ignores it and returns full entries. Additive.
///
/// Minor 12: `branch.graph` returns the full branch topology (every
/// divergent lineage's captures with fork edges, named-branch labels, and
/// logical squash-merge edges) that `sheaf branch list` renders. Additive;
/// an older daemon lacks the capability and the client falls back to a
/// read-only store view.
///
/// Minor 13: `timeline.log` accepts a named `branch`, and its opt-in
/// `details`/`patch` view streams exact parent deltas in the response body.
/// The ordinary JSON capture page remains unchanged. Additive.
pub const PROTO_MINOR: u32 = 13;

/// Maximum size of one JSON envelope frame (1 MiB).
pub const MAX_ENVELOPE: usize = 1024 * 1024;
/// One raw-body chunk ceiling.
pub const MAX_CHUNK: usize = 256 * 1024;

/// One request envelope: protocol version, correlation id, method, the
/// project it concerns, and free-form params.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub v: u32,
    pub id: String,
    pub method: String,
    /// Canonicalized project root this request concerns.
    #[serde(default)]
    pub project: Option<PathBuf>,
    #[serde(default)]
    pub params: Value,
}

/// A structured error carried in a failed response: a stable machine code
/// plus an optional human message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcError {
    pub code: String,
    #[serde(default)]
    pub message: String,
}

impl IpcError {
    /// Build an error from a machine code and a human message.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        IpcError {
            code: code.into(),
            message: message.into(),
        }
    }
}

/// Announces the continuation body that follows a response envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BodyInfo {
    /// Exact number of continuation frames that follow the envelope, or
    /// [`STREAMED_BODY_SENTINEL`] meaning: frames arrive incrementally as
    /// they are produced, terminated by one empty frame. The sentinel and
    /// terminator are the proto-1.5 streamed-body framing; the summary
    /// such methods would normally carry in `result` arrives as the last
    /// non-empty body record instead (the envelope is written before the
    /// work starts, so its `result` can only acknowledge the stream).
    pub chunks: u32,
}

/// `BodyInfo.chunks` marker announcing a streamed body.
pub const STREAMED_BODY_SENTINEL: u32 = u32::MAX;

/// One response envelope correlated to a request by `id`: success flag, an
/// optional result or error, and optional body-continuation info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub v: u32,
    pub id: String,
    pub ok: bool,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub error: Option<IpcError>,
    #[serde(default)]
    pub body: Option<BodyInfo>,
}

impl Response {
    /// Build a successful response carrying `result` for request `id`.
    pub fn ok(id: impl Into<String>, result: Value) -> Self {
        Response {
            v: PROTO_MAJOR,
            id: id.into(),
            ok: true,
            result: Some(result),
            error: None,
            body: None,
        }
    }

    /// Build a failed response carrying error `e` for request `id`.
    pub fn err(id: impl Into<String>, e: IpcError) -> Self {
        Response {
            v: PROTO_MAJOR,
            id: id.into(),
            ok: false,
            result: None,
            error: Some(e),
            body: None,
        }
    }
}

// ---------------------------------------------------------------- framing

/// Write one length-prefixed frame, refusing a payload larger than `cap`.
pub fn write_frame<W: Write>(w: &mut W, payload: &[u8], cap: usize) -> std::io::Result<()> {
    if payload.len() > cap {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame {} bytes exceeds cap {}", payload.len(), cap),
        ));
    }
    w.write_all(&(payload.len() as u32).to_le_bytes())?;
    w.write_all(payload)?;
    w.flush()
}

/// Read one length-prefixed frame, refusing a peer-declared length over `cap`.
pub fn read_frame<R: Read>(r: &mut R, cap: usize) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > cap {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("peer sent oversized frame ({len} > {cap})"),
        ));
    }
    let mut out = vec![0u8; len];
    r.read_exact(&mut out)?;
    Ok(out)
}

// ---------------------------------------------------------------- client

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// A connected IPC client: one Unix-socket stream over which requests are
/// framed out and responses (with optional bodies) read back.
pub struct Client {
    stream: UnixStream,
}

/// Successful exchange: response envelope plus reassembled body bytes.
pub struct Reply {
    pub response: Response,
    pub body: Vec<u8>,
}

impl Client {
    /// Connect to the daemon at `socket`, applying `timeout` to reads and
    /// writes.
    pub fn connect(socket: &Path, timeout: Duration) -> Result<Client> {
        let stream = UnixStream::connect(socket)
            .map_err(|e| SheafError::Ipc(format!("connect {}: {e}", socket.display())))?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        Ok(Client { stream })
    }

    /// Widen the read/write budget after a fast connect — a diff over a
    /// large tree legitimately computes for longer than a handshake.
    pub fn set_timeout(&mut self, timeout: Duration) -> Result<()> {
        self.stream.set_read_timeout(Some(timeout))?;
        self.stream.set_write_timeout(Some(timeout))?;
        Ok(())
    }

    /// Send one request and fully read its response, buffering any body
    /// (streamed or counted) into the returned [`Reply`].
    ///
    /// A response whose id does not match the request is an orphan left by
    /// an earlier call that timed out client-side after its request reached
    /// the daemon: the daemon answered late, and that answer now sits ahead
    /// of ours in the stream. Such stale (older-id) responses are drained —
    /// body and all — so this call still returns its own answer instead of
    /// silently handing back the wrong one.
    pub fn call(
        &mut self,
        method: &str,
        project: Option<&Path>,
        params: Value,
        body: Option<&[u8]>,
    ) -> Result<Reply> {
        let id = self.send_request(method, project, params)?;
        if let Some(bytes) = body {
            // Chunked upload mirrors download framing (reserved; unused v1).
            let mut w = &self.stream;
            for c in bytes.chunks(MAX_CHUNK.max(1)) {
                write_frame(&mut w, c, MAX_CHUNK)?;
            }
        }

        loop {
            let resp = self.read_envelope()?;
            if resp.id == id {
                let mut body_out = Vec::new();
                if let Some(info) = &resp.body {
                    if info.chunks == STREAMED_BODY_SENTINEL {
                        // Streamed body: buffer it whole for this buffered caller.
                        while let Some(chunk) = self.read_stream_chunk()? {
                            body_out.extend_from_slice(&chunk);
                        }
                    } else {
                        for _ in 0..info.chunks {
                            let chunk = read_frame(&mut self.stream, MAX_CHUNK)
                                .map_err(|e| SheafError::Ipc(format!("read body chunk: {e}")))?;
                            body_out.extend_from_slice(&chunk);
                        }
                    }
                }
                return Ok(Reply {
                    response: resp,
                    body: body_out,
                });
            }
            self.drain_orphan(&resp, &id)?;
        }
    }

    /// Like [`Client::call`], but for a streamed-body method: `on_chunk`
    /// fires as each body frame arrives (before the request returns), so
    /// callers render results with scan-time liveness. The returned
    /// `Reply.body` still contains every byte for post-processing.
    pub fn call_streaming(
        &mut self,
        method: &str,
        project: Option<&Path>,
        params: Value,
        on_chunk: &mut dyn FnMut(&[u8]),
    ) -> Result<Reply> {
        let id = self.send_request(method, project, params)?;

        loop {
            let resp = self.read_envelope()?;
            if resp.id == id {
                let mut body_out = Vec::new();
                if let Some(info) = &resp.body {
                    if info.chunks == STREAMED_BODY_SENTINEL {
                        while let Some(chunk) = self.read_stream_chunk()? {
                            on_chunk(&chunk);
                            body_out.extend_from_slice(&chunk);
                        }
                    } else {
                        for _ in 0..info.chunks {
                            let chunk = read_frame(&mut self.stream, MAX_CHUNK)
                                .map_err(|e| SheafError::Ipc(format!("read body chunk: {e}")))?;
                            on_chunk(&chunk);
                            body_out.extend_from_slice(&chunk);
                        }
                    }
                }
                return Ok(Reply {
                    response: resp,
                    body: body_out,
                });
            }
            // A stale response is not the caller's data: drain it without
            // firing `on_chunk`.
            self.drain_orphan(&resp, &id)?;
        }
    }

    /// Read and parse one response envelope frame.
    fn read_envelope(&mut self) -> Result<Response> {
        let env_bytes = read_frame(&mut self.stream, MAX_ENVELOPE)
            .map_err(|e| SheafError::Ipc(format!("read response: {e}")))?;
        serde_json::from_slice(&env_bytes)
            .map_err(|e| SheafError::Ipc(format!("parse response: {e}")))
    }

    /// Discard an orphaned response's body so the stream realigns on the
    /// next envelope. Only stale (older-id) orphans are expected; a response
    /// carrying an id at or beyond the one we are waiting on is a genuine
    /// desync we cannot recover from, so it is a hard error.
    fn drain_orphan(&mut self, resp: &Response, expected: &str) -> Result<()> {
        let stale = match (resp.id.parse::<u64>(), expected.parse::<u64>()) {
            (Ok(got), Ok(want)) => got < want,
            // Non-numeric ids never appear from the daemon; treat any
            // mismatch as unrecoverable.
            _ => false,
        };
        if !stale {
            return Err(SheafError::Ipc(format!(
                "response id {} does not match request id {expected}",
                resp.id
            )));
        }
        if let Some(info) = &resp.body {
            if info.chunks == STREAMED_BODY_SENTINEL {
                while self.read_stream_chunk()?.is_some() {}
            } else {
                for _ in 0..info.chunks {
                    read_frame(&mut self.stream, MAX_CHUNK)
                        .map_err(|e| SheafError::Ipc(format!("drain body chunk: {e}")))?;
                }
            }
        }
        Ok(())
    }

    /// Serialize and frame one request onto the socket, returning its
    /// correlation id so the caller can match the response to it.
    fn send_request(
        &mut self,
        method: &str,
        project: Option<&Path>,
        params: Value,
    ) -> Result<String> {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed).to_string();
        let req = Request {
            v: PROTO_MAJOR,
            id: id.clone(),
            method: method.to_string(),
            project: project.map(|p| p.to_path_buf()),
            params,
        };
        let payload = serde_json::to_vec(&req)
            .map_err(|e| SheafError::Ipc(format!("serialize request: {e}")))?;
        let mut w = &self.stream;
        write_frame(&mut w, &payload, MAX_ENVELOPE)?;
        Ok(id)
    }

    /// Read one streamed-body frame; `None` is the empty terminator.
    fn read_stream_chunk(&mut self) -> Result<Option<Vec<u8>>> {
        let chunk = read_frame(&mut self.stream, MAX_CHUNK)
            .map_err(|e| SheafError::Ipc(format!("read streamed body chunk: {e}")))?;
        if chunk.is_empty() {
            Ok(None)
        } else {
            Ok(Some(chunk))
        }
    }

    /// Liveness probe returning `(major, minor, daemon_version)`.
    pub fn ping(&mut self) -> Result<(u32, u32, String)> {
        let reply = self.call("ping", None, Value::Null, None)?;
        if !reply.response.ok {
            return Err(SheafError::Ipc(err_text(&reply.response)));
        }
        let r = reply.response.result.clone().unwrap_or(Value::Null);
        let proto = r.get("proto").cloned().unwrap_or(Value::Null);
        let major = proto.get("major").and_then(Value::as_u64).unwrap_or(0) as u32;
        let minor = proto.get("minor").and_then(Value::as_u64).unwrap_or(0) as u32;
        let ver = r
            .get("daemon_version")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        Ok((major, minor, ver))
    }

    /// Consume the client and hand back the raw socket for direct use.
    pub fn into_stream(self) -> UnixStream {
        self.stream
    }
}

fn err_text(resp: &Response) -> String {
    resp.error
        .as_ref()
        .map(|e| format!("{}: {}", e.code, e.message))
        .unwrap_or_else(|| "unknown error".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip_and_caps() {
        let mut buf: Vec<u8> = Vec::new();
        write_frame(&mut buf, b"hello", 100).unwrap();
        write_frame(&mut buf, b"", 100).unwrap();
        let mut cur = std::io::Cursor::new(buf);
        assert_eq!(read_frame(&mut cur, 100).unwrap(), b"hello");
        assert_eq!(read_frame(&mut cur, 100).unwrap(), b"");
    }

    #[test]
    fn oversize_rejected_both_directions() {
        let big = vec![7u8; 200];
        let mut buf = Vec::new();
        assert!(write_frame(&mut buf, &big, 100).is_err());
        // Forge an oversized header and refuse to read it back.
        let forged = (500u32).to_le_bytes();
        let mut cur = std::io::Cursor::new(forged.to_vec());
        assert!(read_frame(&mut cur, 100).is_err());
    }

    #[test]
    fn envelope_serde_defaults() {
        let json = br#"{"v":1,"id":"x","ok":false,"error":{"code":"bad.method","message":"nope"}}"#;
        let r: Response = serde_json::from_slice(json).unwrap();
        assert!(!r.ok);
        assert_eq!(r.error.unwrap().code, "bad.method");
        assert!(r.result.is_none() && r.body.is_none());
    }

    #[test]
    fn chunk_body_roundtrip() {
        // Server-side simulation: announce 2 chunks, write them, read back.
        let data = vec![42u8; 300]; // forces 2 chunks at 256KiB? no—cap huge.
        let _ = data;
        let mut wire = Vec::new();
        let chunk_a = [1u8; 10];
        let chunk_b = [2u8; 5];
        write_frame(&mut wire, &chunk_a, MAX_CHUNK).unwrap();
        write_frame(&mut wire, &chunk_b, MAX_CHUNK).unwrap();

        let resp = Response {
            v: 1,
            id: "z".into(),
            ok: true,
            result: None,
            error: None,
            body: Some(BodyInfo { chunks: 2 }),
        };
        let env = serde_json::to_vec(&resp).unwrap();
        let mut full = Vec::new();
        write_frame(&mut full, &env, MAX_ENVELOPE).unwrap();
        full.extend_from_slice(&wire);

        let mut cur = std::io::Cursor::new(full);
        let envb = read_frame(&mut cur, MAX_ENVELOPE).unwrap();
        let parsed: Response = serde_json::from_slice(&envb).unwrap();
        let mut got = Vec::new();
        for _ in 0..parsed.body.as_ref().unwrap().chunks {
            got.extend_from_slice(&read_frame(&mut cur, MAX_CHUNK).unwrap());
        }
        assert_eq!(got.len(), 15);
        assert_eq!(&got[..10], &[1u8; 10][..]);
        assert_eq!(&got[10..], &[2u8; 5][..]);
    }

    // ------------------------------------------------------- client over a
    // real loopback socket. A tiny in-process server thread scripts the
    // daemon side so connect/call/ping paths run end-to-end without a
    // daemon binary.

    use std::os::unix::net::UnixListener;

    /// Bind a fresh socket under `dir` and return its path.
    fn bind_socket(dir: &Path) -> (PathBuf, UnixListener) {
        let path = dir.join("test.sock");
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).expect("bind test socket");
        (path, listener)
    }

    /// Serve exactly one request: read the envelope, hand it to `serve`
    /// together with the connection so the script can reply (and stream).
    fn serve_one(
        listener: UnixListener,
        serve: impl FnOnce(&[u8], &mut UnixStream) + Send + 'static,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let (mut stream, _peer) = listener.accept().expect("accept");
            let req = read_frame(&mut stream, MAX_ENVELOPE).expect("read request");
            serve(&req, &mut stream);
        })
    }

    fn write_env(stream: &mut UnixStream, resp: &Response) {
        let env = serde_json::to_vec(resp).unwrap();
        write_frame(stream, &env, MAX_ENVELOPE).unwrap();
    }

    /// The correlation id the client put on a request. A real daemon always
    /// echoes it on the response; the mock servers must too, now that the
    /// client enforces request→response id matching.
    fn req_id(req: &[u8]) -> String {
        let parsed: Request = serde_json::from_slice(req).expect("parse request");
        parsed.id
    }

    #[test]
    fn ping_roundtrip_returns_protocol_and_version() {
        let tmp = tempfile::tempdir().unwrap();
        let (path, listener) = bind_socket(tmp.path());
        let server = serve_one(listener, |req, stream| {
            write_env(
                stream,
                &Response::ok(
                    req_id(req),
                    serde_json::json!({
                        "proto": {"major": PROTO_MAJOR, "minor": PROTO_MINOR},
                        "daemon_version": "9.9.9-test",
                    }),
                ),
            );
        });
        let mut client = Client::connect(&path, Duration::from_secs(5)).unwrap();
        client.set_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(
            client.ping().unwrap(),
            (PROTO_MAJOR, PROTO_MINOR, "9.9.9-test".to_string())
        );
        server.join().unwrap();
    }

    #[test]
    fn ping_error_reply_maps_to_ipc_error() {
        let tmp = tempfile::tempdir().unwrap();

        // A structured error renders "code: message".
        let (path, listener) = bind_socket(tmp.path());
        let server = serve_one(listener, |req, stream| {
            write_env(
                stream,
                &Response::err(req_id(req), IpcError::new("bad.method", "nope")),
            );
        });
        let mut client = Client::connect(&path, Duration::from_secs(5)).unwrap();
        let err = client.ping().unwrap_err();
        server.join().unwrap();
        assert!(err.to_string().contains("bad.method: nope"), "{err}");

        // A !ok reply with NO error object falls back to "unknown error".
        let (path, listener) = bind_socket(tmp.path());
        let server = serve_one(listener, |req, stream| {
            write_env(
                stream,
                &Response {
                    v: PROTO_MAJOR,
                    id: req_id(req),
                    ok: false,
                    result: None,
                    error: None,
                    body: None,
                },
            );
        });
        let mut client = Client::connect(&path, Duration::from_secs(5)).unwrap();
        let err = client.ping().unwrap_err();
        server.join().unwrap();
        assert!(err.to_string().contains("unknown error"), "{err}");
    }

    #[test]
    fn call_uploads_chunked_body_and_reads_a_counted_body_back() {
        let tmp = tempfile::tempdir().unwrap();
        let (path, listener) = bind_socket(tmp.path());
        let upload: Vec<u8> = vec![7u8; MAX_CHUNK + 7]; // forces 2 upload frames
        let up = upload.clone();
        let server = serve_one(listener, move |req, stream| {
            let req = req_id(req);
            // The request body arrived as MAX_CHUNK-sized continuation frames.
            let a = read_frame(stream, MAX_CHUNK).unwrap();
            let b = read_frame(stream, MAX_CHUNK).unwrap();
            assert_eq!(a.len(), MAX_CHUNK);
            assert_eq!(b, &up[MAX_CHUNK..]);
            // Reply with a counted 2-chunk body.
            let mut resp = Response::ok(req, serde_json::json!({"accepted": true}));
            resp.body = Some(BodyInfo { chunks: 2 });
            write_env(stream, &resp);
            write_frame(stream, b"alpha-", MAX_CHUNK).unwrap();
            write_frame(stream, b"omega", MAX_CHUNK).unwrap();
        });
        let mut client = Client::connect(&path, Duration::from_secs(5)).unwrap();
        let reply = client
            .call(
                "store.append",
                Some(Path::new("/proj")),
                serde_json::json!({}),
                Some(&upload),
            )
            .unwrap();
        server.join().unwrap();
        assert!(reply.response.ok);
        assert_eq!(reply.response.result.unwrap()["accepted"], true);
        assert_eq!(reply.body, b"alpha-omega");
    }

    #[test]
    fn call_streaming_fires_per_chunk_until_the_sentinel_terminator() {
        let tmp = tempfile::tempdir().unwrap();
        let (path, listener) = bind_socket(tmp.path());
        let server = serve_one(listener, |req, stream| {
            let mut resp = Response::ok(req_id(req), Value::Null);
            resp.body = Some(BodyInfo {
                chunks: STREAMED_BODY_SENTINEL,
            });
            write_env(stream, &resp);
            write_frame(stream, b"one", MAX_CHUNK).unwrap();
            write_frame(stream, b"two", MAX_CHUNK).unwrap();
            // Empty frame = stream terminator.
            write_frame(stream, b"", MAX_CHUNK).unwrap();
        });
        let mut client = Client::connect(&path, Duration::from_secs(5)).unwrap();
        let mut seen: Vec<Vec<u8>> = Vec::new();
        let reply = client
            .call_streaming("timeline.grep", None, serde_json::json!({}), &mut |c| {
                seen.push(c.to_vec())
            })
            .unwrap();
        server.join().unwrap();
        assert_eq!(seen, vec![b"one".to_vec(), b"two".to_vec()]);
        assert_eq!(
            reply.body, b"onetwo",
            "buffered copy matches the streamed view"
        );
    }

    #[test]
    fn call_streaming_also_reads_counted_bodies() {
        let tmp = tempfile::tempdir().unwrap();
        let (path, listener) = bind_socket(tmp.path());
        let server = serve_one(listener, |req, stream| {
            let mut resp = Response::ok(req_id(req), Value::Null);
            resp.body = Some(BodyInfo { chunks: 2 });
            write_env(stream, &resp);
            write_frame(stream, b"alpha", MAX_CHUNK).unwrap();
            write_frame(stream, b"beta", MAX_CHUNK).unwrap();
        });
        let mut client = Client::connect(&path, Duration::from_secs(5)).unwrap();
        let mut seen: Vec<Vec<u8>> = Vec::new();
        let reply = client
            .call_streaming("diff.plan", None, serde_json::json!({}), &mut |c| {
                seen.push(c.to_vec())
            })
            .unwrap();
        server.join().unwrap();
        assert_eq!(seen, vec![b"alpha".to_vec(), b"beta".to_vec()]);
        assert_eq!(reply.body, b"alphabeta");
    }

    #[test]
    fn into_stream_hands_back_a_usable_socket() {
        let tmp = tempfile::tempdir().unwrap();
        let (path, listener) = bind_socket(tmp.path());
        // serve_one consumes one frame; echo it straight back.
        let server = serve_one(listener, |req, stream| {
            write_frame(stream, req, MAX_ENVELOPE).unwrap();
        });
        let client = Client::connect(&path, Duration::from_secs(5)).unwrap();
        let mut raw = client.into_stream();
        write_frame(&mut raw, b"echo-me", MAX_ENVELOPE).unwrap();
        let back = read_frame(&mut raw, MAX_ENVELOPE).unwrap();
        assert_eq!(back, b"echo-me");
        server.join().unwrap();
    }

    #[test]
    fn call_drains_a_stale_orphan_before_returning_its_own_response() {
        // The desync a client-side timeout leaves behind: an earlier call
        // gave up after its request reached the daemon, so the daemon's
        // late answer (an older id, here even carrying a body) now sits
        // ahead of this call's real answer. The client must discard the
        // orphan — body and all — and return the response that matches.
        let tmp = tempfile::tempdir().unwrap();
        let (path, listener) = bind_socket(tmp.path());
        let server = serve_one(listener, |req, stream| {
            let want: u64 = req_id(req).parse().unwrap();
            let mut orphan =
                Response::ok((want - 1).to_string(), serde_json::json!({"stale": true}));
            orphan.body = Some(BodyInfo { chunks: 1 });
            write_env(stream, &orphan);
            write_frame(stream, b"orphan-body", MAX_CHUNK).unwrap();
            write_env(
                stream,
                &Response::ok(want.to_string(), serde_json::json!({"fresh": true})),
            );
        });
        let mut client = Client::connect(&path, Duration::from_secs(5)).unwrap();
        let reply = client
            .call("diff", None, serde_json::json!({}), None)
            .unwrap();
        server.join().unwrap();
        assert_eq!(reply.response.result.unwrap()["fresh"], true);
        assert!(
            reply.body.is_empty(),
            "the orphan's body must not leak into the matched reply"
        );
    }

    #[test]
    fn call_rejects_a_response_id_that_is_not_stale() {
        // Only an older id is a forgivable orphan. An id at or beyond the
        // one we are waiting on is a genuine desync we cannot recover from,
        // so it is a hard error rather than a silently-accepted mismatch.
        let tmp = tempfile::tempdir().unwrap();
        let (path, listener) = bind_socket(tmp.path());
        let server = serve_one(listener, |req, stream| {
            let want: u64 = req_id(req).parse().unwrap();
            write_env(
                stream,
                &Response::ok((want + 1).to_string(), serde_json::json!({"future": true})),
            );
        });
        let mut client = Client::connect(&path, Duration::from_secs(5)).unwrap();
        let err = match client.call("diff", None, serde_json::json!({}), None) {
            Ok(_) => panic!("a non-stale id mismatch must be rejected"),
            Err(err) => err,
        };
        server.join().unwrap();
        assert!(
            err.to_string().contains("does not match request id"),
            "{err}"
        );
    }
}

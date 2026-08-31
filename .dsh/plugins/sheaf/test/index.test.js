import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  DEFAULT_CONFIG,
  SHEAF_PROMPT_TEXT,
  SheafError,
  buildToolDefinitions,
  createEnrollmentTracker,
  createSheafApi,
  parseStatus,
  resolveConfig,
  shellQuote,
  truncateText,
} from '../index.js'

// ─── config ────────────────────────────────────────────────────────────────

test('resolveConfig applies defaults and freezes', () => {
  const config = resolveConfig({})
  assert.equal(config.bin, DEFAULT_CONFIG.bin)
  assert.equal(config.allowWrite, false)
  assert.equal(config.timeoutMs, DEFAULT_CONFIG.timeoutMs)
  assert.equal(config.maxOutputChars, DEFAULT_CONFIG.maxOutputChars)
  assert.equal(Object.isFrozen(config), true)
})

test('resolveConfig rejects bad values', () => {
  assert.throws(() => resolveConfig({ bin: ' ' }), TypeError)
  assert.throws(() => resolveConfig({ timeoutMs: 0 }), TypeError)
  assert.throws(() => resolveConfig({ allowWrite: 'yes' }), TypeError)
  assert.throws(() => resolveConfig({ injectPrompt: 'no' }), TypeError)
  assert.throws(() => resolveConfig({ defaultProject: '  ' }), TypeError)
  assert.equal(resolveConfig({}).injectPrompt, true)
})

// ─── pure helpers ──────────────────────────────────────────────────────────

test('shellQuote single-quotes and escapes embedded quotes', () => {
  assert.equal(shellQuote('simple'), "'simple'")
  assert.equal(shellQuote("it's"), "'it'\\''s'")
  assert.equal(shellQuote('2 hours ago'), "'2 hours ago'")
  assert.equal(shellQuote('a$b `cmd` \\path'), "'a$b `cmd` \\path'")
})

test('parseStatus reads health variants', () => {
  const running = parseStatus(
    [
      'project:       /home/me/p',
      'store:         format 1',
      'enrolled:      yes',
      'daemon:        running v0.1.0 (proto 1.1)',
      'watching:      yes',
    ].join('\n'),
  )
  assert.equal(running.enrolled, true)
  assert.equal(running.daemonRunning, true)
  assert.equal(running.watching, true)
  assert.equal(running.pendingRestore, null)
  assert.equal(running.fields.store, 'format 1')

  const dead = parseStatus('enrolled:      yes\ndaemon:        not running (/run/user/1000/sheaf/control.sock)\nwatching:      no')
  assert.equal(dead.daemonRunning, false)
  assert.equal(dead.watching, false)

  const pending = parseStatus('daemon:        running v0.1.0 (proto 1.1)\npending:       restore to @~3 waiting (stale)')
  assert.equal(pending.daemonRunning, true)
  assert.equal(pending.pendingRestore, 'restore to @~3 waiting (stale)')

  const notified = parseStatus('daemon:        notified — watching live')
  assert.equal(notified.daemonRunning, true)
})

test('truncateText caps and marks, passes short text through', () => {
  const short = truncateText('abc', 10)
  assert.equal(short.truncated, false)
  assert.equal(short.text, 'abc')
  const long = truncateText('x'.repeat(25), 10)
  assert.equal(long.truncated, true)
  assert.ok(long.text.startsWith('xxxxxxxxxx'))
  assert.ok(long.text.includes('truncated'))
})

// ─── service API against a fake shell ──────────────────────────────────────

/** Fake Host `shell` service capturing requests and replaying canned runs. */
function fakeCtx(runs) {
  const requests = []
  const shell = {
    resolve: (request) => request,
    run: async (spec) => {
      requests.push(spec)
      const run = runs.length > 0 ? runs.shift() : { exitCode: 0, stdout: '{}\n', stderr: '' }
      return {
        exitCode: run.exitCode ?? 0,
        signal: null,
        timedOut: run.timedOut ?? false,
        aborted: run.aborted ?? false,
        stdout: { text: run.stdout ?? '', truncated: false },
        stderr: { text: run.stderr ?? '', truncated: false },
      }
    },
  }
  return { ctx: { shell }, requests }
}

const AGENT_CWD = { agent: { session: { header: { cwd: '/home/me/proj' } } } }

test('log builds the timeline.log command against the session workspace', async () => {
  const { ctx, requests } = fakeCtx([{ stdout: '{"degraded":false,"entries":[],"tips":1}' }])
  const api = createSheafApi(ctx, resolveConfig({}))
  const result = await api.log({ path: 'src/lib.rs', follow: true, limit: 5, exec: AGENT_CWD })
  assert.deepEqual(result, { degraded: false, entries: [], tips: 1 })
  assert.equal(requests.length, 1)
  assert.equal(requests[0].workdir, '/home/me/proj')
  assert.equal(
    requests[0].command,
    "'sheaf' 'log' '-C' '/home/me/proj' '--json' '--path' 'src/lib.rs' '--follow' '--limit' '5'",
  )
})

test('explicit project wins over session cwd; no_project when nothing resolves', async () => {
  const { ctx, requests } = fakeCtx([{ stdout: '{"checkpoints":[]}' }])
  const api = createSheafApi(ctx, resolveConfig({}))
  await api.checkpointList({ project: '/elsewhere', exec: AGENT_CWD })
  assert.equal(requests[0].workdir, '/elsewhere')

  const bare = fakeCtx([])
  const bareApi = createSheafApi(bare.ctx, resolveConfig({}))
  await assert.rejects(() => bareApi.status({}), (error) => {
    assert.ok(error instanceof SheafError)
    assert.equal(error.code, 'sheaf.no_project')
    return true
  })
})

test('CLI failures surface exit code and stderr', async () => {
  const { ctx } = fakeCtx([{ exitCode: 2, stderr: 'error: no sheaf store found here' }])
  const api = createSheafApi(ctx, resolveConfig({}))
  await assert.rejects(() => api.doctor({ project: '/nope' }), (error) => {
    assert.ok(error instanceof SheafError)
    assert.equal(error.code, 'sheaf.cli_error')
    assert.equal(error.exitCode, 2)
    assert.ok(error.message.includes('no sheaf store found'))
    return true
  })
})

test('write gate denies by default and opens with allowWrite', async () => {
  const denied = createSheafApi(fakeCtx([]).ctx, resolveConfig({}))
  await assert.rejects(() => denied.restoreApply({ at: '@~1', project: '/p' }), (error) => {
    assert.equal(error.code, 'sheaf.gated')
    return true
  })
  await assert.rejects(() => denied.init({ path: '/p' }), (error) => error.code === 'sheaf.gated')
  await assert.rejects(() => denied.gc({ apply: true, project: '/p' }), (error) => error.code === 'sheaf.gated')

  const { ctx, requests } = fakeCtx([{ stdout: '{"plan":{}}' }, { stdout: '{"gc":{}}' }])
  const allowed = createSheafApi(ctx, resolveConfig({ allowWrite: true }))
  await allowed.restorePlan({ at: 'checkpoint:before-x', project: '/p' })
  assert.equal(requests[0].command, "'sheaf' 'restore' '-C' '/p' '--dry-run' '--json' '--at' 'checkpoint:before-x'")
  await allowed.gc({ apply: true, project: '/p' })
  assert.equal(requests[1].command, "'sheaf' 'gc' '-C' '/p' '--json' '--apply'")
})

test('restore apply passes the longer deadline and scoped paths positionally', async () => {
  const { ctx, requests } = fakeCtx([{ stdout: '{"outcome":{}}' }])
  const api = createSheafApi(ctx, resolveConfig({ allowWrite: true }))
  await api.restoreApply({ at: '@~3', paths: ['src/a.rs', 'src/b.rs'], project: '/p' })
  assert.equal(requests[0].command, "'sheaf' 'restore' '-C' '/p' '--json' '--at' '@~3' 'src/a.rs' 'src/b.rs'")
  assert.equal(requests[0].timeoutMs, 180000)
})

test('checkpointCreate quotes names safely and returns output', async () => {
  const { ctx, requests } = fakeCtx([{ stdout: 'checkpoint git-abc123 -> deadbeef\n' }])
  const api = createSheafApi(ctx, resolveConfig({}))
  const result = await api.checkpointCreate({ name: 'git-abc123 fix journal', project: '/p' })
  assert.equal(result.name, 'git-abc123 fix journal')
  assert.equal(result.output, 'checkpoint git-abc123 -> deadbeef')
  assert.equal(requests[0].command, "'sheaf' 'checkpoint' 'create' '-C' '/p' 'git-abc123 fix journal'")
})

test('status parses through the service; overview degrades per part', async () => {
  const statusText = 'enrolled:      yes\ndaemon:        running v0.1.0 (proto 1.1)\nwatching:      yes'
  const { ctx } = fakeCtx([
    { stdout: statusText },                                     // direct status call
    { stdout: statusText },                                     // overview: status
    { stdout: '{"degraded":false,"entries":[{"id":"c1"}],"tips":1}' }, // overview: log
    { exitCode: 3, stderr: 'boom' },                            // overview: checkpointList fails
    { stdout: '{"diff":{"entries":[{"kind":"modified"}]},"patch":"@@ x"}' }, // overview: diff
  ])
  const api = createSheafApi(ctx, resolveConfig({}))
  const status = await api.status({ project: '/p' })
  assert.equal(status.parsed.daemonRunning, true)
  assert.equal(status.project, '/p')

  const overview = await api.overview({ project: '/p' })
  assert.equal(overview.project, '/p')
  assert.equal(overview.recentCaptures.length, 1)
  assert.equal(overview.worktreeDiff.entryCount, 1)
  assert.ok(overview.worktreeDiff.patchPreview.includes('@@ x'))
  assert.ok(String(overview.errors.checkpoints).includes('boom'))
})

// ─── conditional prompt injection ──────────────────────────────────────────

const flush = () => new Promise((resolve) => setImmediate(resolve))
const enrolledAgent = { session: { header: { cwd: '/home/me/proj' } } }

test('enrollment tracker injects guidance only for enrolled workspaces', async () => {
  const { ctx, requests } = fakeCtx([{ exitCode: 0 }])
  const tracker = createEnrollmentTracker(ctx)
  // Unknown agent: silent, but the probe is kicked for the next assembly.
  assert.equal(tracker.textFor({ agent: enrolledAgent }), '')
  assert.equal(requests[0].command, "test -d '/home/me/proj/.sheaf'")
  await flush()
  assert.equal(tracker.textFor({ agent: enrolledAgent }), SHEAF_PROMPT_TEXT)
  // The answer is cached: no second probe.
  tracker.textFor({ agent: enrolledAgent })
  assert.equal(requests.length, 1)
})

test('enrollment tracker stays silent for absent stores and non-agents', async () => {
  const { ctx } = fakeCtx([{ exitCode: 1 }])
  const tracker = createEnrollmentTracker(ctx)
  tracker.textFor({ agent: enrolledAgent })
  await flush()
  assert.equal(tracker.textFor({ agent: enrolledAgent }), '')
  assert.equal(tracker.textFor({}), '')
  assert.equal(tracker.textFor(undefined), '')
})

test('session restart re-probes after reset', async () => {
  const { ctx, requests } = fakeCtx([{ exitCode: 0 }, { exitCode: 1 }])
  const tracker = createEnrollmentTracker(ctx)
  tracker.textFor({ agent: enrolledAgent })
  await flush()
  assert.equal(tracker.textFor({ agent: enrolledAgent }), SHEAF_PROMPT_TEXT)
  tracker.reset(enrolledAgent)
  tracker.probe(enrolledAgent)
  await flush()
  assert.equal(requests.length, 2)
  assert.equal(tracker.textFor({ agent: enrolledAgent }), '')
})

test('enrollment caches by stable session id, not object identity', async () => {
  const { ctx, requests } = fakeCtx([{ exitCode: 0 }])
  const tracker = createEnrollmentTracker(ctx)
  const assemblyAgent = { session: { id: 'sess-1', header: { cwd: '/home/me/proj' } } }
  const sameSessionNewObject = { session: { id: 'sess-1', header: { cwd: '/home/me/proj' } } }
  assert.equal(tracker.textFor({ agent: assemblyAgent }), '')
  await flush()
  // A fresh object for the same session id hits the cache: no second probe.
  assert.equal(tracker.textFor({ agent: sameSessionNewObject }), SHEAF_PROMPT_TEXT)
  assert.equal(requests.length, 1)
})

// ─── tools ─────────────────────────────────────────────────────────────────

test('tool definitions mirror the spec surface and wire through the API', async () => {
  const { ctx, requests } = fakeCtx([{ stdout: '{"degraded":false,"entries":[],"tips":0}' }])
  const api = createSheafApi(ctx, resolveConfig({}))
  const defs = buildToolDefinitions(api)
  const names = defs.map((def) => def.name)
  assert.deepEqual(names, [
    'sheaf_status',
    'sheaf_log',
    'sheaf_info',
    'sheaf_diff',
    'sheaf_checkpoint_list',
    'sheaf_checkpoint_create',
    'sheaf_restore_plan',
    'sheaf_restore_apply',
    'sheaf_doctor',
    'sheaf_gc',
    'sheaf_init',
  ])
  for (const def of defs) {
    assert.equal(typeof def.execute, 'function')
    assert.equal(def.parameters.type, 'object')
    // Dual-mount constraint: harness.defineTool rejects a closed parameter
    // root ("the implicit parameter root is open"), so every tool must keep
    // the root open for the composition mount to stay identical.
    assert.ok(
      def.parameters.additionalProperties === undefined || def.parameters.additionalProperties === true,
      `${def.name} must keep its parameter root open`,
    )
    assert.equal(def.output.schema.type, 'object')
    // Dual-mount constraint: the value-schema compiler rejects an output
    // object whose additionalProperties is not explicitly true or false.
    assert.equal(def.output.schema.additionalProperties, true)
    const blocks = def.output.render({}, { ok: true })
    assert.equal(blocks[0].type, 'text')
  }
  const logTool = defs.find((def) => def.name === 'sheaf_log')
  const value = await logTool.execute({ limit: 2 }, AGENT_CWD)
  assert.deepEqual(value, { degraded: false, entries: [], tips: 0 })
  assert.ok(requests[0].command.includes("'log'"))
  assert.ok(requests[0].command.includes("'--json'"))
  assert.ok(requests[0].command.includes("'-C' '/home/me/proj'"))
})

test('gated tool rejects through execute with the gate message', async () => {
  const api = createSheafApi(fakeCtx([]).ctx, resolveConfig({}))
  const applyTool = buildToolDefinitions(api).find((def) => def.name === 'sheaf_restore_apply')
  await assert.rejects(() => applyTool.execute({ at: '@~1' }, AGENT_CWD), (error) => {
    assert.equal(error.code, 'sheaf.gated')
    assert.ok(error.message.includes('allowWrite'))
    return true
  })
})

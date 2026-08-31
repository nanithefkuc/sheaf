// dsh-sheaf — sheaf integration for DeepSeek Harness (DSH).
//
// sheaf is a daemon-backed flight recorder beneath git; DSH has no MCP
// support, so this plugin wraps the `sheaf` CLI directly (same adapter
// philosophy as sheaf-mcp: CLI-wrapping, default-deny
// writes). It contributes exactly three things:
//
//   1. the Host service `sheaf` — a pure-JSON, per-project read/write API
//      over the CLI. Every method takes plain JSON and returns plain JSON
//      (the same JSON shapes the CLI emits, wherever the CLI emits them).
//      This service is the stable contract future DSH UI reads from:
//      `sheaf.overview()` is the one-call payload a status panel needs,
//      and no host change is required
//      to later add a browser half (Remote descriptors, a webServer route,
//      or a client bundle) because nothing in the API is live-runtime data.
//
//   2. model tools `sheaf_*` mirroring the sheaf-mcp surface (plus
//      sheaf_info), so agents get native tools instead of shelling out.
//
//   3. context injection: a systemPrompt section that appears ONLY for
//      sessions whose workspace is sheaf-enrolled (a `.sheaf/` directory at
//      the session root, probed via the shell seam). Sections whose resolved
//      text is empty are omitted by the assembler, so non-enrolled sessions
//      see nothing. Without this, agents in other enrolled projects get the
//      tools but no usage discipline; the deep docs live in the sheaf skill.
//
// Mounting: HOST PLANE ONLY, as a profile bundle row (see cordis.patch.yml).
// The plugin publishes the `sheaf` service, so a loose agent-preset row would
// publish it process-global and be rejected at mount; a realm would hide it
// from the host/UI plane. Install:
//   dsh plugin --profile web add "link:$PWD/.dsh/plugins/sheaf"
//
// Dual-mount testing: this module is dependency-free plain JavaScript with
// every `export` as a line prefix, so stripping those prefixes and appending
// `return { name, inject, apply }` yields a valid dynamic-plugin package
// body (verified in-session before every bundle change). Do not introduce
// imports, require, or non-prefix exports.

export const name = 'sheaf'
export const inject = ['shell']

/** Service key published on the Host plane; the read surface for future UI. */
export const SERVICE_NAME = 'sheaf'

/** Plugin API version; bump on any breaking change to the service shape. */
export const API_VERSION = 1

export const DEFAULT_CONFIG = Object.freeze({
  // CLI binary (mirrors the SHEAF_BIN override the CLI honors).
  bin: 'sheaf',
  // Default-deny: restoreApply / gc(apply) / init refuse unless true, because
  // each rewrites the worktree or timeline and needs an explicit opt-in.
  allowWrite: false,
  // Project root used when a call names none. Default: the calling agent's
  // session workspace (tools), else this, else the call fails with
  // `sheaf.no_project`.
  defaultProject: undefined,
  // Per-call timeout (diff may legitimately take 30s).
  timeoutMs: 60000,
  // Output cap in characters per result, so one huge patch cannot flood the
  // model context.
  maxOutputChars: 200000,
  // Register the conditional system-prompt guidance for enrolled sessions.
  injectPrompt: true,
})

/** Stable identity for the injected prompt section. */
export const PROMPT_SECTION_NAME = 'sheaf:guidance'

/** Prompt order: after baseline policy sections and far after persona (0). */
export const PROMPT_SECTION_ORDER = 85

/**
 * The injected guidance, distilled from the sheaf skill: enough discipline
 * for an agent that has never seen sheaf before, short enough to sit in
 * every prompt of every enrolled session.
 */
export const SHEAF_PROMPT_TEXT = `## Sheaf flight recorder (this workspace is enrolled)

Every change to this worktree is captured continuously on an append-only timeline beneath git; git remains the tool of record. Navigate it with the sheaf_* tools instead of shelling out.

- Pin a checkpoint BEFORE risky work (sheaf_checkpoint_create), named after intent, not dates. Checkpoints are instant annotations that never rewrite history; find them with sheaf_checkpoint_list.
- Browse history with sheaf_log (timeline points: capture-ID prefix, @ = last capture, @~N, or a timestamp); sheaf_info gives one capture's file-level detail.
- sheaf_diff reviews a work unit (sheaf_diff checkpoint:<anchor> --stat shows exactly what the next commit will collapse); sheaf_restore_plan previews a non-destructive rewind.
- Restores never erase: the pre-restore state is captured and the abandoned future stays reachable. After a restore, do NOT "clean up" with git checkout . — commit the rewound state or restore forward.
- Write tools (sheaf_restore_apply, sheaf_init, sheaf_gc with apply=true) are denied unless the operator enables allowWrite.
- Never commit .sheaf/; it is excluded via .git/info/exclude. sheaf_status reports daemon health — captures and timeline writes need the daemon.`

/** Hard ceiling for per-call timeout overrides (restore.apply has a 120s deadline, plus slack). */
export const MAX_TIMEOUT_MS = 180000

const GATE_HINT =
  'Write tools are denied by default. Set allowWrite: true on the dsh-sheaf host row to enable it.'

/**
 * Thrown for every sheaf API failure. `code` is a stable dotted string;
 * `exitCode`/`stderr`/`command` are attached when a CLI run failed. Tool
 * wrappers stringify this into the model-visible error text.
 */
export class SheafError extends Error {
  constructor(code, message, extra = {}) {
    super(message)
    this.name = 'SheafError'
    this.code = code
    this.exitCode = extra.exitCode
    this.stderr = extra.stderr
    this.command = extra.command
  }
}

/** Validate and freeze plugin config against DEFAULT_CONFIG. */
export function resolveConfig(input = {}) {
  if (input === null || typeof input !== 'object' || Array.isArray(input)) {
    throw new TypeError('dsh-sheaf: config must be an object')
  }
  const config = { ...DEFAULT_CONFIG, ...input }
  if (typeof config.bin !== 'string' || config.bin.trim() === '') {
    throw new TypeError('dsh-sheaf: `bin` must be a non-empty string')
  }
  if (config.defaultProject !== undefined) {
    if (typeof config.defaultProject !== 'string' || config.defaultProject.trim() === '') {
      throw new TypeError('dsh-sheaf: `defaultProject` must be a non-empty string or omitted')
    }
  }
  for (const key of ['timeoutMs', 'maxOutputChars']) {
    if (!Number.isSafeInteger(config[key]) || config[key] < 1) {
      throw new TypeError(`dsh-sheaf: \`${key}\` must be a positive integer`)
    }
  }
  if (typeof config.allowWrite !== 'boolean') {
    throw new TypeError('dsh-sheaf: `allowWrite` must be a boolean')
  }
  if (typeof config.injectPrompt !== 'boolean') {
    throw new TypeError('dsh-sheaf: `injectPrompt` must be a boolean')
  }
  config.bin = config.bin.trim()
  return Object.freeze(config)
}

/** POSIX single-quote an argument for the shell seam ('' + '\'' style). */
export function shellQuote(value) {
  const text = String(value)
  return `'${text.replace(/'/g, "'\\''")}'`
}

/**
 * Tolerant parser for the human `sheaf status` output (the one subcommand
 * without --json; a CLI follow-up may replace this — keep the `raw` text in
 * results so consumers can re-derive). Lines are `key:   value`; unknown
 * keys pass through untouched.
 */
export function parseStatus(text) {
  const fields = {}
  for (const line of String(text).split('\n')) {
    const index = line.indexOf(':')
    if (index <= 0) continue
    const key = line.slice(0, index).trim()
    if (key === '') continue
    fields[key] = line.slice(index + 1).trim()
  }
  const daemon = typeof fields.daemon === 'string' ? fields.daemon : ''
  return {
    fields,
    enrolled: fields.enrolled === 'yes',
    daemonRunning: daemon.startsWith('running') || daemon.startsWith('notified'),
    watching: fields.watching === 'yes',
    pendingRestore: typeof fields.pending === 'string' && fields.pending !== '' ? fields.pending : null,
  }
}

/** Cap text at maxChars, marking the result when truncated. */
export function truncateText(text, maxChars) {
  const value = String(text)
  if (value.length <= maxChars) return { text: value, truncated: false }
  const cut = value.slice(0, maxChars)
  const note = `\n… [dsh-sheaf] output truncated (${value.length - maxChars} more chars; raise maxOutputChars or narrow the query)`
  return { text: cut + note, truncated: true }
}

function firstJsonError(stdout, command) {
  const excerpt = String(stdout).slice(0, 400)
  return new SheafError(
    'sheaf.bad_json',
    `\`${command}\` was expected to emit JSON but did not; output head: ${excerpt}`,
    { command },
  )
}

/**
 * Build the `sheaf` service API. `ctx` must carry the Host `shell` service
 * (declared via inject). Every method is async, accepts plain JSON, and
 * resolves to plain JSON — the contract future UI depends on. `exec` (with
 * `exec.agent.session.header.cwd`) is passed through by the tool wrappers so
 * model calls default to the session workspace.
 */
export function createSheafApi(ctx, config) {
  const timeoutFor = (override) => {
    if (override === undefined) return config.timeoutMs
    if (!Number.isSafeInteger(override) || override < 1 || override > MAX_TIMEOUT_MS) {
      throw new SheafError('sheaf.bad_params', `timeoutMs must be a positive integer ≤ ${MAX_TIMEOUT_MS}`)
    }
    return override
  }

  function resolveProject(explicit, exec) {
    if (typeof explicit === 'string' && explicit.trim() !== '') return explicit.trim()
    const cwd = exec?.agent?.session?.header?.cwd
    if (typeof cwd === 'string' && cwd !== '') return cwd
    if (typeof config.defaultProject === 'string' && config.defaultProject !== '') {
      return config.defaultProject
    }
    throw new SheafError(
      'sheaf.no_project',
      'No sheaf project resolved: pass `project`, or set `defaultProject` in the dsh-sheaf config.',
    )
  }

  function requireWrite(what) {
    if (!config.allowWrite) throw new SheafError('sheaf.gated', `${what} is denied: ${GATE_HINT}`)
  }

  /**
   * Run the CLI. Args are quoted individually and joined; the resolved
   * project root becomes the working directory so the CLI's own
   * nearest-ancestor store discovery applies.
   */
  async function runSheaf(args, opts = {}) {
    if (!Array.isArray(args) || args.some((a) => typeof a !== 'string')) {
      throw new SheafError('sheaf.bad_params', 'args must be an array of strings')
    }
    const command = [config.bin, ...args].map(shellQuote).join(' ')
    const request = {
      command,
      timeoutMs: timeoutFor(opts.timeoutMs),
      stdoutMaxBytes: config.maxOutputChars * 4,
    }
    if (opts.workdir !== undefined) request.workdir = opts.workdir
    if (opts.signal !== undefined) request.signal = opts.signal
    const result = await ctx.shell.run(ctx.shell.resolve(request))
    const stdout = typeof result.stdout?.text === 'string' ? result.stdout.text : ''
    const stderr = typeof result.stderr?.text === 'string' ? result.stderr.text : ''
    if (result.timedOut) {
      throw new SheafError('sheaf.timeout', `\`${command}\` timed out after ${request.timeoutMs}ms`, { command, stderr })
    }
    if (result.aborted) {
      throw new SheafError('sheaf.aborted', `\`${command}\` was aborted`, { command })
    }
    if (result.exitCode !== 0) {
      const detail = (stderr.trim() !== '' ? stderr.trim() : stdout.trim()) || `exit ${String(result.exitCode)}`
      throw new SheafError('sheaf.cli_error', `\`${command}\` failed (exit ${String(result.exitCode)}): ${detail}`, {
        exitCode: result.exitCode,
        stderr,
        command,
      })
    }
    return { command, stdout, stderr }
  }

  async function runJson(args, opts) {
    const { command, stdout } = await runSheaf(args, opts)
    try {
      return JSON.parse(stdout)
    } catch {
      throw firstJsonError(stdout, command)
    }
  }

  // `-C/--project` is a SUBCOMMAND flag in the sheaf CLI (the root command
  // takes none), so every argv below splices it after the subcommand chain.
  const projectFlag = (project) => ['-C', project]

  const api = {
    plugin: 'dsh-sheaf',
    apiVersion: API_VERSION,
    config: Object.freeze({ ...config }),

    /** Low-level escape hatch: run arbitrary CLI args, return raw output. */
    async run({ args, project, timeoutMs, signal } = {}) {
      const root = project === undefined ? undefined : resolveProject(project)
      const r = await runSheaf(args, { workdir: root, timeoutMs, signal })
      const capped = truncateText(r.stdout, config.maxOutputChars)
      return { exitCode: 0, command: r.command, stdout: capped.text, truncated: capped.truncated, stderr: r.stderr }
    },

    /** Store + daemon health. Parsed from human output (no --json yet). */
    async status({ project, exec, signal } = {}) {
      const root = resolveProject(project, exec)
      const r = await runSheaf(['status'], { workdir: root, signal })
      return { project: root, parsed: parseStatus(r.stdout), raw: r.stdout }
    },

    /** Browse captures (the `timeline.log` verb; newest first). */
    async log({ project, path, follow, all, before, limit, exec, signal } = {}) {
      const root = resolveProject(project, exec)
      const args = ['log', ...projectFlag(root), '--json']
      if (path !== undefined) args.push('--path', String(path))
      if (follow === true) args.push('--follow')
      if (all === true) args.push('--all')
      if (before !== undefined) args.push('--before', String(before))
      if (limit !== undefined) {
        if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) {
          throw new SheafError('sheaf.bad_params', 'limit must be an integer in 1..1000')
        }
        args.push('--limit', String(limit))
      }
      return runJson(args, { workdir: root, signal })
    },

    /** File-level detail of one capture (the `timeline.info` verb). */
    async info({ project, reference, exec, signal } = {}) {
      if (typeof reference !== 'string' || reference.trim() === '') {
        throw new SheafError('sheaf.bad_params', 'reference is required (capture-ID prefix, @, @~N)')
      }
      const root = resolveProject(project, exec)
      return runJson(['info', ...projectFlag(root), '--json', reference.trim()], { workdir: root, signal })
    },

    /** Worktree-vs-point or point-vs-point diff; `--json` embeds the patch. */
    async diff({ project, from, to, paths, stat, exec, signal } = {}) {
      const root = resolveProject(project, exec)
      const args = ['diff', ...projectFlag(root), '--json']
      if (stat === true) args.push('--stat')
      if (from !== undefined) args.push(String(from))
      if (to !== undefined) args.push(String(to))
      if (Array.isArray(paths)) for (const p of paths) args.push('--path', String(p))
      const outcome = await runJson(args, { workdir: root, timeoutMs: undefined, signal })
      const capped = truncateText(outcome.patch ?? '', config.maxOutputChars)
      return { ...outcome, patch: capped.text, patchTruncated: capped.truncated }
    },

    /** Named checkpoints (the `checkpoint.list` verb). */
    async checkpointList({ project, exec, signal } = {}) {
      const root = resolveProject(project, exec)
      return runJson(['checkpoint', 'list', ...projectFlag(root), '--json'], { workdir: root, signal })
    },

    /** Pin a name to a timeline point (a timeline write, but ungated: it only annotates, never rewrites the tree). */
    async checkpointCreate({ project, name, at, exec, signal } = {}) {
      if (typeof name !== 'string' || name.trim() === '') {
        throw new SheafError('sheaf.bad_params', 'name is required')
      }
      const root = resolveProject(project, exec)
      const args = ['checkpoint', 'create', ...projectFlag(root), name.trim()]
      if (at !== undefined) args.push('--at', String(at))
      const r = await runSheaf(args, { workdir: root, signal })
      return { project: root, name: name.trim(), output: r.stdout.trim() }
    },

    /** Dry-run restore: pure computation, never touches the worktree. */
    async restorePlan({ project, at, paths, exec, signal } = {}) {
      if (typeof at !== 'string' || at.trim() === '') {
        throw new SheafError('sheaf.bad_params', 'at is required (capture ID, checkpoint:<name>, @, @~N, or time)')
      }
      const root = resolveProject(project, exec)
      const args = ['restore', ...projectFlag(root), '--dry-run', '--json', '--at', at.trim()]
      if (Array.isArray(paths)) for (const p of paths) args.push(String(p))
      return runJson(args, { workdir: root, timeoutMs: undefined, signal })
    },

    /** Execute a restore (repositions the worktree; undoable, since the pre-restore state is captured first). Gated. */
    async restoreApply({ project, at, paths, exec, signal } = {}) {
      requireWrite('sheaf_restore_apply')
      if (typeof at !== 'string' || at.trim() === '') {
        throw new SheafError('sheaf.bad_params', 'at is required (capture ID, checkpoint:<name>, @, @~N, or time)')
      }
      const root = resolveProject(project, exec)
      const args = ['restore', ...projectFlag(root), '--json', '--at', at.trim()]
      if (Array.isArray(paths)) for (const p of paths) args.push(String(p))
      return runJson(args, { workdir: root, timeoutMs: MAX_TIMEOUT_MS, signal })
    },

    /** Integrity sweep (read-only). */
    async doctor({ project, exec, signal } = {}) {
      const root = resolveProject(project, exec)
      return runJson(['doctor', ...projectFlag(root), '--json'], { workdir: root, signal })
    },

    /** Retention report, or collection with apply:true. Gated when applying. */
    async gc({ project, apply, exec, signal } = {}) {
      if (apply === true) requireWrite('sheaf_gc(apply=true)')
      const root = resolveProject(project, exec)
      const args = ['gc', ...projectFlag(root), '--json']
      if (apply === true) args.push('--apply')
      return runJson(args, { workdir: root, signal })
    },

    /** Enroll a directory. Gated. */
    async init({ path, exec, signal } = {}) {
      requireWrite('sheaf_init')
      if (typeof path !== 'string' || path.trim() === '') {
        throw new SheafError('sheaf.bad_params', 'path is required')
      }
      // No workdir: the target may not exist yet; the CLI takes the path arg.
      const r = await runSheaf(['init', path.trim()], { signal })
      return { path: path.trim(), output: r.stdout.trim() }
    },

    /**
     * One-call panel payload for future UI: status + recent captures +
     * checkpoints + uncaptured worktree diff, all best-effort (a failing
     * part lands in `errors` instead of failing the whole call).
     */
    async overview({ project, exec, signal } = {}) {
      const root = resolveProject(project, exec)
      const opts = { project: root, exec, signal }
      const [statusR, logR, checkpointsR, diffR] = await Promise.allSettled([
        api.status(opts),
        api.log({ ...opts, limit: 5 }),
        api.checkpointList(opts),
        api.diff(opts),
      ])
      const value = (r) => (r.status === 'fulfilled' ? r.value : undefined)
      const errors = {}
      for (const [part, r] of [['status', statusR], ['log', logR], ['checkpoints', checkpointsR], ['worktreeDiff', diffR]]) {
        if (r.status === 'rejected') errors[part] = r.reason instanceof Error ? r.reason.message : String(r.reason)
      }
      const worktreeDiff = value(diffR)
      let diffSummary = undefined
      if (worktreeDiff !== undefined) {
        const patch = worktreeDiff.patch ?? ''
        diffSummary = {
          entries: worktreeDiff.diff?.entries ?? [],
          entryCount: (worktreeDiff.diff?.entries ?? []).length,
          patchPreview: patch.length > 2000 ? `${patch.slice(0, 2000)}\n… [preview truncated]` : patch,
        }
      }
      const log = value(logR)
      return {
        project: root,
        degraded: log?.degraded,
        status: value(statusR),
        recentCaptures: log?.entries ?? [],
        tips: log?.tips,
        checkpoints: value(checkpointsR)?.checkpoints ?? [],
        worktreeDiff: diffSummary,
        errors: Object.keys(errors).length === 0 ? undefined : errors,
      }
    },
  }
  return api
}

/**
 * Tool definitions in final registry shape (JSON-Schema `parameters`,
 * `output.schema`, `execute(args, exec)`). Registered identically by the
 * composition mount (ctx `tools` service) and the dynamic mount
 * (harness.registerTool).
 */
export function buildToolDefinitions(api) {
  const projectParam = {
    type: 'string',
    description: 'Project root directory. Default: the session workspace, resolving to the nearest sheaf-enrolled ancestor.',
  }
  // Output objects are open maps by design (CLI JSON passes through), and
  // the schema compiler requires additionalProperties to be EXPLICIT.
  const objectType = { type: 'object', additionalProperties: true }

  const defs = []
  const add = (def) => defs.push(def)

  add({
    name: 'sheaf_status',
    description:
      'sheaf store + daemon health for a project: enrolled, watching, daemon version, pending restore intent. Use before any timeline write to confirm the daemon is running.',
    parameters: {
      type: 'object',
      properties: { project: projectParam },
    },
    output: { schema: objectType, render: (_a, v) => [{ type: 'text', text: JSON.stringify(v, null, 2) }] },
    execute: (args, exec) => api.status({ ...args, exec }),
  })

  add({
    name: 'sheaf_log',
    description:
      'Browse sheaf capture history (newest first): every debounced burst of worktree changes beneath git. Timeline points: capture-ID prefix, checkpoint:<name>, @ (last capture), @~N, or a timestamp.',
    parameters: {
      type: 'object',
      properties: {
        project: projectParam,
        path: { type: 'string', description: 'Only captures touching this root-relative path' },
        follow: { type: 'boolean', description: 'Follow renames: include captures under former names of --path' },
        all: { type: 'boolean', description: 'Include divergent (abandoned-future) branches, not only the current lineage' },
        before: { type: 'string', description: 'Pagination cursor: continue after this capture-ID prefix' },
        limit: { type: 'number', description: 'Max entries (1..1000, default 50)' },
      },
    },
    output: { schema: objectType, render: (_a, v) => [{ type: 'text', text: JSON.stringify(v, null, 2) }] },
    execute: (args, exec) => api.log({ ...args, exec }),
  })

  add({
    name: 'sheaf_info',
    description:
      'File-level detail of one capture: which paths changed and how, versus its exact parent frontier. Reference: capture-ID prefix, @, or @~N.',
    parameters: {
      type: 'object',
      properties: { reference: { type: 'string', description: 'Capture reference (@, @~N, or capture-ID prefix)' }, project: projectParam },
      required: ['reference'],
    },
    output: { schema: objectType, render: (_a, v) => [{ type: 'text', text: JSON.stringify(v, null, 2) }] },
    execute: (args, exec) => api.info({ ...args, exec }),
  })

  add({
    name: 'sheaf_diff',
    description:
      'Compare the worktree against a timeline point, or two points (pass from and to, or "A..B" as from). No points: uncaptured worktree edits vs the last capture. stat:true gives a per-file summary.',
    parameters: {
      type: 'object',
      properties: {
        project: projectParam,
        from: { type: 'string', description: 'Old point (default @); "A..B" compares two points' },
        to: { type: 'string', description: 'New point; omit to compare from against the live worktree' },
        paths: { type: 'array', items: { type: 'string' }, description: 'Limit to these root-relative paths' },
        stat: { type: 'boolean', description: 'Per-file summary instead of a patch' },
      },
    },
    output: { schema: objectType, render: (_a, v) => [{ type: 'text', text: JSON.stringify(v, null, 2) }] },
    execute: (args, exec) => api.diff({ ...args, exec }),
  })

  add({
    name: 'sheaf_checkpoint_list',
    description: 'List named sheaf checkpoints (pins over exact timeline points, with lineage membership).',
    parameters: {
      type: 'object',
      properties: { project: projectParam },
    },
    output: { schema: objectType, render: (_a, v) => [{ type: 'text', text: JSON.stringify(v, null, 2) }] },
    execute: (args, exec) => api.checkpointList({ ...args, exec }),
  })

  add({
    name: 'sheaf_checkpoint_create',
    description:
      'Pin a name to the current timeline point (default @). Create BEFORE risky work, named after intent (before-parser-rework, not a date). Instant, never rewrites history.',
    parameters: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Checkpoint name; name the intent, not the date' },
        at: { type: 'string', description: 'Timeline point to pin (default @)' },
        project: projectParam,
      },
      required: ['name'],
    },
    output: { schema: objectType, render: (_a, v) => [{ type: 'text', text: JSON.stringify(v, null, 2) }] },
    execute: (args, exec) => api.checkpointCreate({ ...args, exec }),
  })

  add({
    name: 'sheaf_restore_plan',
    description:
      'Dry-run a non-destructive restore: full ordered plan (writes, deletes, obstructions, undo capture) without touching the worktree. Always plan before sheaf_restore_apply.',
    parameters: {
      type: 'object',
      properties: {
        at: { type: 'string', description: 'Target point: capture ID, checkpoint:<name>, @, @~N, or timestamp' },
        paths: { type: 'array', items: { type: 'string' }, description: 'Scope: only these root-relative paths move (omit for full-tree)' },
        project: projectParam,
      },
      required: ['at'],
    },
    output: { schema: objectType, render: (_a, v) => [{ type: 'text', text: JSON.stringify(v, null, 2) }] },
    execute: (args, exec) => api.restorePlan({ ...args, exec }),
  })

  add({
    name: 'sheaf_restore_apply',
    description:
      'Execute a restore: reposition the worktree to a timeline point, non-destructively (pre-restore state is captured; the abandoned future stays reachable). Plan first with sheaf_restore_plan. WRITE-GATED: needs allowWrite in the dsh-sheaf host config.',
    parameters: {
      type: 'object',
      properties: {
        at: { type: 'string', description: 'Target point: capture ID, checkpoint:<name>, @, @~N, or timestamp' },
        paths: { type: 'array', items: { type: 'string' }, description: 'Scope: only these paths move (omit for full-tree)' },
        project: projectParam,
      },
      required: ['at'],
    },
    output: { schema: objectType, render: (_a, v) => [{ type: 'text', text: JSON.stringify(v, null, 2) }] },
    execute: (args, exec) => api.restoreApply({ ...args, exec }),
  })

  add({
    name: 'sheaf_doctor',
    description: 'Read-only store integrity sweep: journal framing, snapshot chain, blob coverage, capture/branch counts.',
    parameters: {
      type: 'object',
      properties: { project: projectParam },
    },
    output: { schema: objectType, render: (_a, v) => [{ type: 'text', text: JSON.stringify(v, null, 2) }] },
    execute: (args, exec) => api.doctor({ ...args, exec }),
  })

  add({
    name: 'sheaf_gc',
    description:
      'Retention: report collectable bytes, or collect with apply:true. The plan never removes anything any restore could still need. apply:true is WRITE-GATED (needs allowWrite).',
    parameters: {
      type: 'object',
      properties: { apply: { type: 'boolean', description: 'Actually collect (default: report only)' }, project: projectParam },
    },
    output: { schema: objectType, render: (_a, v) => [{ type: 'text', text: JSON.stringify(v, null, 2) }] },
    execute: (args, exec) => api.gc({ ...args, exec }),
  })

  add({
    name: 'sheaf_init',
    description:
      'Enroll a directory: create its .sheaf/ store skeleton and tell the daemon. WRITE-GATED: needs allowWrite in the dsh-sheaf host config.',
    parameters: {
      type: 'object',
      properties: { path: { type: 'string', description: 'Directory to enroll' } },
      required: ['path'],
    },
    output: { schema: objectType, render: (_a, v) => [{ type: 'text', text: JSON.stringify(v, null, 2) }] },
    execute: (args, exec) => api.init({ ...args, exec }),
  })

  return defs
}

/**
 * Per-agent enrollment state for the conditional prompt section. Cached by
 * the agent's STABLE session id when present (string key — immune to any
 * per-assembly object identity), falling back to the agent object in a
 * WeakMap. `probe` fires an async `test -d <cwd>/.sheaf` through the shell
 * seam (exit 1 is a legitimate "absent", so this bypasses runSheaf's error
 * mapping) and the cache holds undefined (unknown) → 'pending' → true/false.
 * `textFor` is synchronous by contract (the assembler calls it inline):
 * unknown agents kick a probe and stay silent until the next assembly, which
 * is at most one model step late.
 */
export function createEnrollmentTracker(ctx) {
  const byId = new Map()
  const byObject = new WeakMap()

  const cacheKey = (agent) => {
    const id = agent?.session?.id ?? agent?.id
    return typeof id === 'string' && id !== '' ? id : null
  }
  const read = (agent) => (cacheKey(agent) === null ? byObject.get(agent) : byId.get(cacheKey(agent)))
  const write = (agent, value) => {
    const id = cacheKey(agent)
    if (id === null) byObject.set(agent, value)
    else byId.set(id, value)
  }
  const forget = (agent) => {
    const id = cacheKey(agent)
    if (id === null) byObject.delete(agent)
    else byId.delete(id)
  }

  function probe(agent) {
    const cwd = agent?.session?.header?.cwd
    if (agent === null || typeof agent !== 'object' || typeof cwd !== 'string' || cwd === '') return
    if (read(agent) !== undefined) return
    write(agent, 'pending')
    try {
      const request = ctx.shell.resolve({
        command: `test -d ${shellQuote(`${cwd}/.sheaf`)}`,
        timeoutMs: 5000,
      })
      ctx.shell.run(request).then(
        (result) => {
          write(agent, result.exitCode === 0)
          console.log(`dsh-sheaf enrollment probe ${cwd} -> exit ${String(result.exitCode)} (${result.exitCode === 0 ? 'enrolled' : 'absent'})`)
        },
        (error) => {
          // Shell hiccup: forget the pending mark so a later assembly retries.
          if (read(agent) === 'pending') forget(agent)
          console.log(`dsh-sheaf enrollment probe ${cwd} failed: ${error?.message ?? String(error)}`)
        },
      )
    } catch (error) {
      forget(agent)
      console.log(`dsh-sheaf enrollment probe threw synchronously: ${error?.message ?? String(error)}`)
    }
  }

  return {
    probe,
    reset(agent) {
      forget(agent)
    },
    textFor(assembleContext) {
      const agent = assembleContext?.agent
      if (agent === null || typeof agent !== 'object') return ''
      const state = read(agent)
      if (state === undefined) {
        probe(agent)
        return ''
      }
      return state === true ? SHEAF_PROMPT_TEXT : ''
    },
  }
}

/**
 * Plugin lifecycle. Registers the `sheaf` service and the sheaf_* tools on
 * the current fiber; Cordis disposes both with it.
 */
export function apply(ctx, inputConfig = {}) {
  const config = resolveConfig(inputConfig)
  const api = createSheafApi(ctx, config)

  ctx.effect(() => ctx.provide(SERVICE_NAME, api))

  // Conditional context injection: only sessions whose workspace has a
  // .sheaf/ store see the guidance (empty text = section omitted). systemPrompt
  // is read defensively so the plugin still degrades to tools-only elsewhere.
  if (config.injectPrompt) {
    const systemPrompt = ctx.get('systemPrompt')
    console.log(`dsh-sheaf injectPrompt=${String(config.injectPrompt)}: systemPrompt ${systemPrompt === undefined ? 'UNAVAILABLE on this plane' : 'available'}`)
    if (systemPrompt !== undefined) {
      const tracker = createEnrollmentTracker(ctx)
      ctx.on('agent/session-start', ({ agent }) => {
        tracker.reset(agent)
        tracker.probe(agent)
      })
      ctx.effect(() =>
        systemPrompt.section({
          name: PROMPT_SECTION_NAME,
          order: PROMPT_SECTION_ORDER,
          text: (assembleContext) => tracker.textFor(assembleContext),
        }),
      )
    }
  }

  const defs = buildToolDefinitions(api)
  const harnessApi = typeof harness !== 'undefined' ? harness : undefined
  if (harnessApi !== undefined && typeof harnessApi.registerTool === 'function') {
    for (const def of defs) {
      ctx.effect(() => harnessApi.registerTool(ctx, harnessApi.defineTool ? harnessApi.defineTool(def) : def))
    }
  } else {
    const tools = ctx.get('tools')
    if (tools === undefined) throw new Error('dsh-sheaf: the tools registry is unavailable on this plane')
    for (const def of defs) {
      ctx.effect(() => tools.register(def))
    }
  }
}

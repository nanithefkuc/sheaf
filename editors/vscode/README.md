# Sheaf for VS Code

Persistent editor-buffer capture and native-first `Ctrl+Z` fall-through into
Sheaf's text history, for VS Code, VSCodium, Cursor, Windsurf, and compatible
forks whose **workspace** extension host runs on Linux.

While you type, the extension records unsaved buffer states into the running
Sheaf daemon (300 ms idle / 2 s hard cap by default, or the project's
`[watch]` values). `Ctrl+Z` and `Ctrl+Shift+Z` first run VS Code's built-in
undo/redo; only when the native stack is exhausted does the keypress fall
through into persistent Sheaf text history.

## Prerequisites

- A matching `sheaf` / `sheafd` build whose `ping` advertises
  `project.resolve`, `editor.capture`, and `editor.step`. Older daemons keep
  serving their existing methods, and this extension stays inactive until all
  three capabilities are present.
- The project is enrolled with `sheaf init`.
- The Sheaf daemon (`sheafd`) is running and watching the project.

The extension only activates for **regular, non-symlink UTF-8 text files** whose
path is classified `Durable` and whose size is at most **1 MiB**. Larger,
binary, symlinked, virtual, untitled, or volatile files keep VS Code's built-in
undo unchanged and continue through the filesystem watcher.

## Build and install the VSIX locally

```sh
npm ci --prefix editors/vscode
npm --prefix editors/vscode run package   # produces sheaf-0.1.0.vsix
code --install-extension editors/vscode/sheaf-0.1.0.vsix
```

There is no Marketplace/Open VSX publishing automation; the locally built VSIX
is the artifact.

## Status bar

- `$(history) Sheaf` — the active file is tracked.
- `$(warning) Sheaf` — the project is enrolled but the daemon is unavailable or
  the file is unsupported. Click it to run **Sheaf: Reconnect**.

`Sheaf: Show Output` reveals stable errors plus capability/version details. It
never logs document content.

## Platform support

The daemon is Linux-only. A Windows or macOS editor connected to a Linux
Remote/WSL/SSH/Codespaces workspace is supported because the extension declares
`extensionKind: ["workspace"]`, so its socket client runs next to the Linux
daemon. If a fork cannot run workspace extensions there, the `sheaf.editorActive`
context stays false and native undo is untouched.

## Development

```sh
npm --prefix editors/vscode run check         # type-check only
npm --prefix editors/vscode run test:unit     # pure Node unit tests
npm --prefix editors/vscode run test:integration
```

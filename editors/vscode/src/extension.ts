import * as vscode from "vscode";

import { IpcError, ProjectResolveReply, resolveSocketPath, SheafClient } from "./ipc";
import { DocumentTracker, TrackerHost } from "./tracker";

const CONTEXT_ACTIVE = "sheaf.editorActive";
const WARMING_RETRY_MS = 250;
const WARMING_BUDGET_MS = 2000;
const OFFLINE_PROBE_MS = 5000;

let manager: Manager | undefined;

export function activate(context: vscode.ExtensionContext): void {
  manager = new Manager();
  manager.register(context);
}

export function deactivate(): Promise<void> {
  const current = manager;
  manager = undefined;
  return current ? current.dispose() : Promise.resolve();
}

/**
 * Owns the single daemon connection, the per-document trackers, and the
 * `sheaf.editorActive` context key that gates the Ctrl+Z override.
 */
class Manager {
  private readonly output = vscode.window.createOutputChannel("Sheaf");
  private readonly status = vscode.window.createStatusBarItem(
    vscode.StatusBarAlignment.Right,
    100,
  );
  private readonly client: SheafClient;
  private readonly trackers = new Map<string, DocumentTracker>();
  private readonly ineligible = new Set<string>();
  private offlineProbe: NodeJS.Timeout | undefined;

  constructor() {
    this.client = new SheafClient(resolveSocketPath(), () => this.onDisconnect());
    this.status.command = "sheaf.reconnect";
  }

  register(context: vscode.ExtensionContext): void {
    context.subscriptions.push(
      this.output,
      this.status,
      { dispose: () => this.client.dispose() },
      vscode.commands.registerCommand("sheaf.undo", () => this.undo()),
      vscode.commands.registerCommand("sheaf.redo", () => this.redo()),
      vscode.commands.registerCommand("sheaf.reconnect", () => this.reconnect()),
      vscode.commands.registerCommand("sheaf.showOutput", () => this.output.show(true)),
      vscode.window.onDidChangeActiveTextEditor((editor) => this.onActiveEditor(editor)),
      vscode.workspace.onDidChangeTextDocument((event) => this.onChange(event)),
      vscode.workspace.onDidSaveTextDocument((document) => this.onSave(document)),
      vscode.workspace.onWillSaveTextDocument((event) => this.onWillSave(event)),
      vscode.workspace.onDidCloseTextDocument((document) => this.onClose(document)),
    );
    void this.onActiveEditor(vscode.window.activeTextEditor);
  }

  async dispose(): Promise<void> {
    clearTimeout(this.offlineProbe);
    for (const tracker of this.trackers.values()) {
      await tracker.flushPending();
      tracker.dispose();
    }
    this.trackers.clear();
    this.client.dispose();
  }

  private log(message: string): void {
    this.output.appendLine(`[${new Date().toISOString()}] ${message}`);
  }

  private hostFor(key: string): TrackerHost {
    return {
      client: this.client,
      log: (message) => this.log(message),
      setActive: (active) => this.updateContext(key, active),
    };
  }

  private updateContext(key: string, active: boolean): void {
    const editor = vscode.window.activeTextEditor;
    const focused = editor !== undefined && editor.document.uri.toString() === key;
    if (focused) {
      void vscode.commands.executeCommand("setContext", CONTEXT_ACTIVE, active);
      this.status.text = active ? "$(history) Sheaf" : "$(warning) Sheaf";
      this.status.show();
    }
  }

  private eligibleUri(document: vscode.TextDocument): boolean {
    if (document.isUntitled || document.uri.fsPath.length === 0) {
      return false;
    }
    if (document.uri.scheme === "file") {
      return true;
    }
    if (vscode.env.remoteName === undefined) {
      return false;
    }
    const folder = vscode.workspace.getWorkspaceFolder(document.uri);
    return folder !== undefined && folder.uri.scheme === document.uri.scheme;
  }

  private async onActiveEditor(editor: vscode.TextEditor | undefined): Promise<void> {
    if (editor === undefined) {
      void vscode.commands.executeCommand("setContext", CONTEXT_ACTIVE, false);
      this.status.hide();
      return;
    }
    const key = editor.document.uri.toString();
    const existing = this.trackers.get(key);
    if (existing) {
      this.updateContext(key, existing.isActive());
      return;
    }
    if (this.ineligible.has(key)) {
      this.status.hide();
      return;
    }
    await this.evaluate(editor.document);
  }

  private async evaluate(document: vscode.TextDocument): Promise<void> {
    if (!this.eligibleUri(document)) {
      return;
    }
    const key = document.uri.toString();
    if (this.trackers.has(key) || this.ineligible.has(key)) {
      return;
    }
    let resolved: ProjectResolveReply;
    try {
      resolved = await this.resolveWithWarming(document.uri.fsPath);
    } catch (error) {
      this.onResolveError(key, error);
      return;
    }
    if (!resolved.registered || !resolved.eligibility.supported) {
      this.ineligible.add(key);
      if (resolved.registered) {
        this.showWarning();
      }
      return;
    }
    const tracker = new DocumentTracker(this.hostFor(key), document, resolved);
    this.trackers.set(key, tracker);
    try {
      // Recheck the same document/version is still active before enabling.
      const active = vscode.window.activeTextEditor;
      if (active === undefined || active.document.uri.toString() !== key) {
        await tracker.activate();
        return;
      }
      const version = active.document.version;
      await tracker.activate();
      if (active.document.version === version) {
        this.updateContext(key, tracker.isActive());
      }
    } catch (error) {
      this.log(`activation failed: ${describe(error)}`);
      this.trackers.delete(key);
      tracker.dispose();
      this.onResolveError(key, error);
    }
  }

  private async resolveWithWarming(fsPath: string): Promise<ProjectResolveReply> {
    const deadline = Date.now() + WARMING_BUDGET_MS;
    for (;;) {
      try {
        return await this.client.resolve(fsPath);
      } catch (error) {
        if (
          error instanceof IpcError &&
          error.code === "project.warming" &&
          Date.now() < deadline
        ) {
          await delay(WARMING_RETRY_MS);
          continue;
        }
        throw error;
      }
    }
  }

  private onResolveError(key: string, error: unknown): void {
    if (error instanceof IpcError && error.code === "project.not_enrolled") {
      this.ineligible.add(key);
      this.status.hide();
      return;
    }
    this.showWarning();
    this.scheduleOfflineProbe();
    this.log(`resolve failed: ${describe(error)}`);
  }

  private showWarning(): void {
    this.status.text = "$(warning) Sheaf";
    this.status.show();
    void vscode.commands.executeCommand("setContext", CONTEXT_ACTIVE, false);
  }

  private scheduleOfflineProbe(): void {
    if (this.offlineProbe !== undefined) {
      return;
    }
    this.offlineProbe = setTimeout(() => {
      this.offlineProbe = undefined;
      const editor = vscode.window.activeTextEditor;
      if (editor) {
        void this.evaluate(editor.document);
      }
    }, OFFLINE_PROBE_MS);
  }

  private onChange(event: vscode.TextDocumentChangeEvent): void {
    const tracker = this.trackers.get(event.document.uri.toString());
    tracker?.onChange(event);
  }

  private onSave(document: vscode.TextDocument): void {
    this.trackers.get(document.uri.toString())?.onSave();
  }

  private onWillSave(event: vscode.TextDocumentWillSaveEvent): void {
    this.trackers.get(event.document.uri.toString())?.onWillSave();
  }

  private async onClose(document: vscode.TextDocument): Promise<void> {
    const key = document.uri.toString();
    const tracker = this.trackers.get(key);
    if (tracker) {
      await tracker.flushPending();
      tracker.dispose();
      this.trackers.delete(key);
    }
    this.ineligible.delete(key);
  }

  private onDisconnect(): void {
    for (const tracker of this.trackers.values()) {
      tracker.exitTraversal();
    }
    void vscode.commands.executeCommand("setContext", CONTEXT_ACTIVE, false);
    this.showWarning();
  }

  private activeTracker(): { editor: vscode.TextEditor; tracker: DocumentTracker } | undefined {
    const editor = vscode.window.activeTextEditor;
    if (editor === undefined) {
      return undefined;
    }
    const tracker = this.trackers.get(editor.document.uri.toString());
    if (tracker === undefined || !tracker.isActive()) {
      return undefined;
    }
    return { editor, tracker };
  }

  private async undo(): Promise<void> {
    const target = this.activeTracker();
    if (target === undefined) {
      await vscode.commands.executeCommand("undo");
      return;
    }
    const { editor, tracker } = target;
    if (tracker.inTraversalMode()) {
      const stepped = await tracker.stepUndo();
      if (!stepped && !tracker.isActive()) {
        await vscode.commands.executeCommand("undo");
      }
      return;
    }
    const before = editor.document.version;
    await vscode.commands.executeCommand("undo");
    if (editor.document.version !== before) {
      return;
    }
    await tracker.stepUndo();
  }

  private async redo(): Promise<void> {
    const target = this.activeTracker();
    if (target === undefined) {
      await vscode.commands.executeCommand("redo");
      return;
    }
    const { editor, tracker } = target;
    if (tracker.inTraversalMode()) {
      const stepped = await tracker.stepRedo();
      if (!stepped && !tracker.isActive()) {
        await vscode.commands.executeCommand("redo");
      }
      return;
    }
    const before = editor.document.version;
    await vscode.commands.executeCommand("redo");
    if (editor.document.version !== before) {
      return;
    }
    if (tracker.hasRedoTarget()) {
      await tracker.stepRedo();
    }
  }

  private async reconnect(): Promise<void> {
    this.client.dispose();
    clearTimeout(this.offlineProbe);
    this.offlineProbe = undefined;
    const editor = vscode.window.activeTextEditor;
    if (editor) {
      this.ineligible.delete(editor.document.uri.toString());
      await this.evaluate(editor.document);
    }
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function describe(error: unknown): string {
  if (error instanceof IpcError) {
    return `${error.code}: ${error.message}`;
  }
  return error instanceof Error ? error.message : String(error);
}

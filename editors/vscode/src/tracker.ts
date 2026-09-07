import * as vscode from "vscode";

import { IpcError, ProjectResolveReply, sha256, SheafClient } from "./ipc";

const INTERACTIVE_DEADLINE_MS = 500;
const DEFAULT_DEBOUNCE_MS = 300;
const DEFAULT_MAX_HOLD_MS = 2000;

export interface TrackerHost {
  readonly client: SheafClient;
  log(message: string): void;
  setActive(active: boolean): void;
}

/**
 * One eligible document's capture pipeline and native-first undo state
 * machine. Every daemon interaction runs through a per-document serialized
 * queue so bursts, saves, and traversal never interleave on the wire.
 */
export class DocumentTracker {
  private queue: Promise<void> = Promise.resolve();
  private idleTimer: NodeJS.Timeout | undefined;
  private hardTimer: NodeJS.Timeout | undefined;
  private pendingEdit = false;
  private savedSha256: string | null = null;
  private baseSha256 = "";
  private cursor: string | null = null;
  private active = false;
  private disposed = false;
  private inTraversal = false;
  private readonly redoStack: string[] = [];
  private applyingEdit = false;
  /** Acknowledged content digest -> logical cursor for this native session. */
  private readonly acknowledged = new Map<string, string>();
  private readonly debounceMs: number;
  private readonly maxHoldMs: number;

  constructor(
    private readonly host: TrackerHost,
    readonly document: vscode.TextDocument,
    private readonly project: ProjectResolveReply,
  ) {
    this.debounceMs = project.watch.debounce_ms || DEFAULT_DEBOUNCE_MS;
    this.maxHoldMs = project.watch.max_hold_ms || DEFAULT_MAX_HOLD_MS;
    if (!document.isDirty) {
      this.savedSha256 = sha256(document.getText());
    }
  }

  isActive(): boolean {
    return this.active;
  }

  isApplyingEdit(): boolean {
    return this.applyingEdit;
  }

  private get root(): string {
    return this.project.root;
  }

  private get relativePath(): string {
    return this.project.relative_path;
  }

  /**
   * First contact: send the current buffer so the daemon establishes this
   * document's logical cursor. Identical content records nothing; a
   * pre-existing dirty buffer becomes recoverable. Activation waits on the
   * daemon accepting the buffer, then rechecks the URI/version still match.
   */
  async activate(): Promise<void> {
    const text = this.document.getText();
    const digest = sha256(text);
    try {
      const reply = await this.host.client.editorCapture(
        this.root,
        {
          path: this.relativePath,
          kind: "edit",
          base_sha256: digest,
          saved_sha256: this.savedSha256,
          source: null,
          target: null,
        },
        text,
      );
      this.baseSha256 = reply.content_sha256;
      this.cursor = reply.cursor;
      if (reply.cursor) {
        this.acknowledged.set(reply.content_sha256, reply.cursor);
      }
      this.active = true;
      this.host.setActive(true);
    } catch (error) {
      this.active = false;
      this.host.setActive(false);
      if (error instanceof IpcError && error.code === "editor.unsupported") {
        this.host.log(`document unsupported: ${error.message}`);
        return;
      }
      throw error;
    }
  }

  /** Route one content change. Undo/redo reasons capture immediately. */
  onChange(event: vscode.TextDocumentChangeEvent): void {
    if (event.contentChanges.length === 0) {
      return;
    }
    if (this.applyingEdit) {
      return;
    }
    if (event.reason === vscode.TextDocumentChangeReason.Undo) {
      this.captureNavigation("undo");
      return;
    }
    if (event.reason === vscode.TextDocumentChangeReason.Redo) {
      this.captureNavigation("redo");
      return;
    }
    // Ordinary edit: leave Sheaf traversal and drop the Sheaf redo stack.
    this.inTraversal = false;
    this.redoStack.length = 0;
    this.pendingEdit = true;
    this.restartIdleTimer();
    if (this.hardTimer === undefined) {
      this.hardTimer = setTimeout(() => this.flushBurst(), this.maxHoldMs);
    }
  }

  /** After a save, refresh the echo digest and enqueue a no-op-safe capture. */
  onSave(): void {
    const digest = sha256(this.document.getText());
    this.savedSha256 = digest;
    this.pendingEdit = true;
    void this.flushBurst();
  }

  onWillSave(): void {
    void this.flushBurst();
  }

  private restartIdleTimer(): void {
    clearTimeout(this.idleTimer);
    this.idleTimer = setTimeout(() => this.flushBurst(), this.debounceMs);
  }

  private clearTimers(): void {
    if (this.idleTimer !== undefined) {
      clearTimeout(this.idleTimer);
      this.idleTimer = undefined;
    }
    if (this.hardTimer !== undefined) {
      clearTimeout(this.hardTimer);
      this.hardTimer = undefined;
    }
  }

  /** Collapse the burst into one ordinary capture of the latest buffer. */
  private flushBurst(): Promise<void> {
    this.clearTimers();
    if (!this.pendingEdit) {
      return Promise.resolve();
    }
    this.pendingEdit = false;
    return this.enqueue(() => this.captureEdit());
  }

  private async captureEdit(): Promise<void> {
    if (this.disposed) {
      return;
    }
    const text = this.document.getText();
    const digest = sha256(text);
    try {
      const reply = await this.host.client.editorCapture(
        this.root,
        {
          path: this.relativePath,
          kind: "edit",
          base_sha256: this.baseSha256,
          saved_sha256: this.savedSha256,
          source: this.cursor,
          target: null,
        },
        text,
      );
      this.baseSha256 = reply.content_sha256;
      this.cursor = reply.cursor;
      if (reply.cursor) {
        this.acknowledged.set(digest, reply.cursor);
      }
    } catch (error) {
      this.handleTransportError(error);
    }
  }

  private captureNavigation(kind: "undo" | "redo"): void {
    const text = this.document.getText();
    const digest = sha256(text);
    const target = this.acknowledged.get(digest) ?? null;
    if (target === null) {
      // VS Code grouped the change differently from Sheaf's timer; without a
      // known cursor we cannot invent a navigation reference, so record it as
      // an ordinary edit instead.
      this.pendingEdit = true;
      void this.flushBurst();
      return;
    }
    void this.enqueue(async () => {
      try {
        const reply = await this.host.client.editorCapture(
          this.root,
          {
            path: this.relativePath,
            kind,
            base_sha256: this.baseSha256,
            saved_sha256: this.savedSha256,
            source: this.cursor,
            target,
          },
          text,
        );
        this.baseSha256 = reply.content_sha256;
        this.cursor = reply.rebased ? reply.cursor : target;
      } catch (error) {
        this.handleTransportError(error);
      }
    });
  }

  /** Serialize one operation behind the document's queue. */
  private enqueue<T>(op: () => Promise<T>): Promise<T> {
    const run = this.queue.then(op);
    this.queue = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  private async drainQueue(deadlineMs: number): Promise<void> {
    await Promise.race([
      this.queue,
      new Promise<void>((resolve) => setTimeout(resolve, deadlineMs)),
    ]);
  }

  /**
   * Persistent undo step: request the previous logical text and apply it as a
   * whole-buffer edit, then record the navigation capture. Returns true when a
   * buffer change was applied. Bounded by the interactive deadline.
   */
  async stepUndo(): Promise<boolean> {
    return this.step("undo");
  }

  async stepRedo(): Promise<boolean> {
    return this.step("redo");
  }

  private async step(direction: "undo" | "redo"): Promise<boolean> {
    if (this.cursor === null) {
      return false;
    }
    const deadline = Date.now() + INTERACTIVE_DEADLINE_MS;
    await this.drainQueue(INTERACTIVE_DEADLINE_MS);
    if (Date.now() >= deadline) {
      this.exitTraversal();
      return false;
    }
    const currentText = this.document.getText();
    const currentSha = sha256(currentText);
    const target = direction === "redo" ? this.redoStack[this.redoStack.length - 1] : undefined;
    if (direction === "redo" && target === undefined) {
      return false;
    }
    try {
      const step = await this.host.client.editorStep(this.root, {
        path: this.relativePath,
        cursor: this.cursor,
        direction,
        target: target ?? null,
        current_sha256: currentSha,
      });
      if (!step.changed) {
        if (direction === "redo") {
          this.redoStack.pop();
        }
        return false;
      }
      const applied = await this.applyBuffer(step.text);
      if (!applied) {
        this.exitTraversal();
        return false;
      }
      await this.recordTraversal(direction, currentSha, step.text, step.source, step.target);
      return true;
    } catch (error) {
      this.host.log(`editor.${direction} failed: ${describe(error)}`);
      this.exitTraversal();
      return false;
    }
  }

  private async applyBuffer(text: string): Promise<boolean> {
    const editor = vscode.window.activeTextEditor;
    if (editor === undefined || editor.document.uri.toString() !== this.document.uri.toString()) {
      return false;
    }
    const version = this.document.version;
    this.applyingEdit = true;
    try {
      const whole = new vscode.Range(
        this.document.positionAt(0),
        this.document.positionAt(this.document.getText().length),
      );
      return await editor.edit(
        (builder) => builder.replace(whole, text),
        { undoStopBefore: true, undoStopAfter: true },
      ).then((ok) => ok && this.document.version !== version);
    } finally {
      this.applyingEdit = false;
    }
  }

  private async recordTraversal(
    direction: "undo" | "redo",
    fromSha: string,
    text: string,
    source: string,
    target: string,
  ): Promise<void> {
    const previousCursor = this.cursor;
    try {
      const reply = await this.host.client.editorCapture(
        this.root,
        {
          path: this.relativePath,
          kind: direction,
          base_sha256: fromSha,
          saved_sha256: this.savedSha256,
          source,
          target,
        },
        text,
      );
      this.baseSha256 = reply.content_sha256;
      this.cursor = reply.rebased ? reply.cursor : target;
      this.acknowledged.set(reply.content_sha256, this.cursor ?? target);
    } catch (error) {
      this.handleTransportError(error);
      return;
    }
    this.inTraversal = true;
    if (direction === "undo") {
      if (previousCursor !== null) {
        this.redoStack.push(previousCursor);
      }
    } else {
      this.redoStack.pop();
    }
  }

  inTraversalMode(): boolean {
    return this.inTraversal;
  }

  hasRedoTarget(): boolean {
    return this.redoStack.length > 0;
  }

  exitTraversal(): void {
    this.inTraversal = false;
    this.redoStack.length = 0;
    this.active = false;
    this.host.setActive(false);
  }

  private handleTransportError(error: unknown): void {
    this.host.log(`editor capture failed: ${describe(error)}`);
    this.exitTraversal();
  }

  /** Flush any pending burst before deactivation/close. */
  async flushPending(): Promise<void> {
    await this.flushBurst();
    await this.queue;
  }

  dispose(): void {
    this.disposed = true;
    this.clearTimers();
    this.active = false;
  }
}

function describe(error: unknown): string {
  if (error instanceof IpcError) {
    return `${error.code}: ${error.message}`;
  }
  return error instanceof Error ? error.message : String(error);
}

import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";

import * as vscode from "vscode";

function workspaceRoot(): string {
  const folder = vscode.workspace.workspaceFolders?.[0];
  assert.ok(folder, "the test workspace must be opened");
  return folder.uri.fsPath;
}

async function settle(ms: number): Promise<void> {
  // Integration exception: activation runs on VS Code's own event loop with
  // no exposed completion signal, so a bounded real delay is unavoidable here.
  await new Promise((resolve) => setTimeout(resolve, ms));
}

suite("Sheaf extension host", () => {
  test("activates and registers its commands", async () => {
    const commands = await vscode.commands.getCommands(true);
    for (const id of ["sheaf.undo", "sheaf.redo", "sheaf.reconnect", "sheaf.showOutput"]) {
      assert.ok(commands.includes(id), `missing command ${id}`);
    }
  });

  test("sheaf.undo delegates to native undo and reverts a fresh edit", async () => {
    const file = path.join(workspaceRoot(), "seed.txt");
    fs.writeFileSync(file, "one\n");
    const document = await vscode.workspace.openTextDocument(file);
    const editor = await vscode.window.showTextDocument(document);
    // Give activation and project.resolve a moment to run (with or without a
    // live daemon; either way native undo must remain correct).
    await settle(500);

    const before = document.getText();
    await editor.edit((builder) =>
      builder.insert(new vscode.Position(document.lineCount - 1, 0), "typed line\n"),
    );
    assert.notEqual(document.getText(), before);

    await vscode.commands.executeCommand("sheaf.undo");
    assert.equal(document.getText(), before, "the fresh edit must be undone");
  });
});

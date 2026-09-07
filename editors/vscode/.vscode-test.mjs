import { defineConfig } from "@vscode/test-cli";

const workspace = process.env.SHEAF_VSCODE_TEST_WORKSPACE;

export default defineConfig([
  {
    label: "integration",
    files: "out/test/integration/*.test.js",
    version: "1.85.0",
    extensionDevelopmentPath: ".",
    workspaceFolder: workspace,
    mocha: {
      timeout: 60000,
    },
  },
]);

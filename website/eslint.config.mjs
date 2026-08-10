import { defineConfig, globalIgnores } from "@eslint/config-helpers";
import foxglove from "@foxglove/eslint-plugin";
import globals from "globals";

export default defineConfig([
  { linterOptions: { reportUnusedDisableDirectives: "error" } },
  globalIgnores([
    ".docusaurus",
    "/build",
    "dist",
    "node_modules",
    "build",
    ".docusaurus",
  ]),
  foxglove.configs.base,
  foxglove.configs.react,
  foxglove.configs.jest,
  {
    languageOptions: {
      globals: { ...globals.es2022 },
      parserOptions: { ecmaVersion: 2022 },
    },
    rules: {
      "no-warning-comments": [
        "error",
        { terms: ["fixme"], location: "anywhere" },
      ],
    },
  },
  {
    files: ["navigation.js", "docusaurus.config.js"],
    languageOptions: {
      globals: { ...globals.node },
    },
  },
  // Enable typescript rules only on typescript files
  foxglove.configs.typescript.map((cfg) => ({
    ...cfg,
    files: ["**/*.@(ts|tsx|cts|mts)"],
  })),
  {
    files: ["**/*.@(ts|tsx|cts|mts)"],
    languageOptions: {
      parserOptions: {
        project: "tsconfig.json",
        tsconfigRootDir: import.meta.dirname,
      },
    },
  },
]);

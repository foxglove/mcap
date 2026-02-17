/* eslint-env node */
module.exports = {
  env: { es2020: true },
  ignorePatterns: ["dist"],
  extends: ["plugin:@foxglove/base", "plugin:@foxglove/jest"],
  overrides: [
    {
      files: ["*.ts", "*.tsx"],
      extends: ["plugin:@foxglove/typescript"],
      parserOptions: {
        project: "tsconfig.json",
        tsconfigRootDir: __dirname,
      },
    },
  ],
  rules: {
    "no-warning-comments": ["error", { terms: ["fixme"], location: "anywhere" }],
  },
};

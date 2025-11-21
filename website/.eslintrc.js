/* eslint-env node */
module.exports = {
  env: {
    es2022: true,
  },
  parserOptions: {
    ecmaVersion: 2022,
  },
  ignorePatterns: ["dist", "node_modules", "build", ".docusaurus"],
  extends: [
    "plugin:@foxglove/base",
    "plugin:@foxglove/react",
    "plugin:@foxglove/jest",
    "plugin:react-hooks/recommended",
  ],
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
    "no-warning-comments": [
      "error",
      {
        terms: ["fixme"],
        location: "anywhere",
      },
    ],
  },
};

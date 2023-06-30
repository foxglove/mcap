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
        project: ["tsconfig.json", "../../*/tsconfig.json"],
        tsconfigRootDir: __dirname,
        // Enable typescript-eslint to use `src` files for type information across project references
        // <https://github.com/typescript-eslint/typescript-eslint/issues/2094>
        EXPERIMENTAL_useSourceOfProjectReferenceRedirect: true,
      },
    },
  ],
  rules: {
    "no-warning-comments": ["error", { terms: ["fixme"], location: "anywhere" }],
  },
};

/* eslint-env node */
/* cspell:disable */
// @ts-nocheck

/**
 * Modify the svgo configuration (in place) to prevent it from minifying IDs in SVGs.
 * This is necessary because it doesn't account for the global ID namespace, and causes
 * ID collisions between the SVGs loaded into the same page.
 *
 * Refs:
 * - https://github.com/facebook/docusaurus/issues/8297
 * - https://github.com/svg/svgo/issues/1714
 * - https://linear.app/foxglove/issue/FG-7251/logos-are-cut-off-on-mcapdev
 *
 * @param {webpack.Configuration} config
 */
module.exports = function modifySvgoConfigInPlace(config) {
  const NEW_SVGO_CONFIG = {
    plugins: [
      {
        name: "preset-default",
        params: {
          overrides: {
            removeTitle: false,
            removeViewBox: false,
            cleanupIDs: false, // do not change IDs
          },
        },
      },
    ],
  };
  // try to find the svgo config rule and replace it
  let updated = false;
  try {
    for (const rule of config.module.rules) {
      if (rule.test?.toString() === "/\\.svg$/i") {
        for (const nestedRule of rule.oneOf) {
          for (const loader of nestedRule.use) {
            if (loader.loader === require.resolve("@svgr/webpack")) {
              loader.options.svgoConfig = NEW_SVGO_CONFIG;
              updated = true;
            }
          }
        }
      }
    }
  } catch (e) {
    console.error("Error while attempting to modify svgo config: " + e);
  }
  if (!updated) {
    throw new Error("Failed to update svgo config");
  }
};

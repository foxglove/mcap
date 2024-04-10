// @ts-check
/* eslint-env node */
/* eslint-disable filenames/match-exported */

const path = require("path");
const darkCodeTheme = require("prism-react-renderer/themes/dracula");
const lightCodeTheme = require("prism-react-renderer/themes/github");
const util = require("util");
const webpack = require("webpack");
const execAsync = util.promisify(require("child_process").exec);

/**
 * Modify the svgo configuration (in place) to prevent it from minifying IDs in SVGs
 *
 * Refs:
 * - https://github.com/facebook/docusaurus/issues/8297
 * - https://github.com/svg/svgo/issues/1714
 * - https://linear.app/foxglove/issue/FG-7251/logos-are-cut-off-on-mcapdev
 *
 * @param {webpack.Configuration} config
 */
function modifySvgoConfig(config) {
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
  // find the svgo config rule and replace it
  if (config.module?.rules instanceof Array) {
    for (const rule of config.module.rules) {
      if (typeof rule === "object" && rule.test?.toString() === "/\\.svg$/i") {
        if (rule.oneOf instanceof Array) {
          for (const nestedRule of rule.oneOf) {
            if (nestedRule.use instanceof Array) {
              for (const loader of nestedRule.use) {
                if (
                  typeof loader === "object" &&
                  /* cspell:disable */
                  loader.loader === require.resolve("@svgr/webpack")
                ) {
                  if (typeof loader.options === "object") {
                    loader.options.svgoConfig = NEW_SVGO_CONFIG;
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: "MCAP",
  tagline: "MCAP file format",
  favicon: "img/favicon32.png",
  url: "https://mcap.dev",
  baseUrl: "/",
  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "throw",

  // disable index.html files because Cloudflare Pages adds a trailing slash
  // https://docusaurus.io/docs/api/docusaurus-config#trailingSlash
  // https://github.com/slorber/trailing-slash-guide
  // https://community.cloudflare.com/t/cloudflare-pages-get-rid-of-redundat-308-redirect/324582
  trailingSlash: false,

  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  plugins: [
    (_context, _options) => ({
      name: "MCAP website custom webpack config",
      configureWebpack(config, _isServer, _utils, _content) {
        // Update config.module.rules directly.
        // (Unclear if this is possible with a mergeStrategy below.)
        modifySvgoConfig(config);

        return {
          mergeStrategy: {
            "resolve.extensions": "replace",
          },
          module: {
            rules: [{ test: /\.wasm$/, type: "asset/resource" }],
          },
          resolve: {
            extensions:
              // Having .wasm as an auto-detected extension for imports breaks some
              // @foxglove/wasm-zstd behavior
              config.resolve?.extensions?.filter((ext) => ext !== ".wasm") ??
              [],
            alias: {
              "@mcap/core": path.resolve(__dirname, "../typescript/core/src"),
            },
            fallback: {
              path: require.resolve("path-browserify"),
              fs: false,
            },
          },
          plugins: [
            new webpack.ProvidePlugin({
              Buffer: ["buffer", "Buffer"],
              process: "process/browser",
            }),
          ],
        };
      },
    }),
    () => {
      // determines the current CLI download link to display by fetching the latest tag matching
      // releases/mcap-cli/* at build time.
      return {
        name: "latestCLIReleaseTag",
        async loadContent() {
          /* cspell:disable */
          const result = await execAsync(
            `git tag --sort=-creatordate --list "releases/mcap-cli/*"`,
          );
          /* cspell:enable */
          if (result.stdout.length === 0) {
            throw new Error(
              `could not determine latest MCAP CLI tag ${JSON.stringify(
                result,
              )}`,
            );
          }
          const latest = result.stdout.split("\n")[0];
          return latest;
        },
        async contentLoaded({ content, actions }) {
          actions.setGlobalData({ tag: content });
        },
      };
    },
  ],

  presets: [
    [
      "classic",
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          routeBasePath: "/",
          sidebarPath: require.resolve("./navigation.js"),
          editUrl: "https://github.com/foxglove/mcap/tree/main/website/",
        },
        blog: false,
        theme: {
          customCss: require.resolve("./src/css/custom.css"),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      image: "img/og-image.jpeg",
      navbar: {
        title: "MCAP",
        logo: {
          alt: "MCAP Logo",
          src: "img/mcap240.webp",
        },
        items: [
          {
            type: "docSidebar",
            sidebarId: "guidesSidebar",
            position: "left",
            label: "Guides",
          },
          {
            type: "docSidebar",
            sidebarId: "referenceSidebar",
            position: "left",
            label: "API Reference",
          },
          {
            type: "docSidebar",
            sidebarId: "specSidebar",
            position: "left",
            label: "Specification",
          },
          {
            href: "https://foxglove.dev/slack",
            label: "Slack",
            position: "right",
          },
          {
            href: "https://github.com/foxglove/mcap",
            label: "GitHub",
            position: "right",
          },
        ],
      },
      footer: {
        style: "dark",
        links: [
          {
            title: "Docs",
            items: [
              {
                label: "Guides",
                to: "/guides",
              },
              {
                label: "API Reference",
                to: "/reference",
              },
              {
                label: "Specification",
                to: "/spec",
              },
            ],
          },
          {
            title: "Community",
            items: [
              {
                label: "GitHub",
                href: "https://github.com/foxglove/mcap",
              },
              {
                label: "Slack",
                href: "https://foxglove.dev/slack",
              },
              {
                label: "Stack Overflow",
                href: "https://stackoverflow.com/questions/tagged/mcap",
              },
              {
                label: "Robotics Stack Exchange",
                href: "https://robotics.stackexchange.com/questions/tagged/mcap",
              },
            ],
          },
          {
            title: "Enterprise",
            items: [
              {
                label: "Foxglove",
                href: "https://foxglove.dev/",
              },
            ],
          },
        ],
        copyright: `Copyright &copy; <a href="https://foxglove.dev" style="color: inherit">Foxglove</a>`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
      colorMode: {
        respectPrefersColorScheme: true,
      },
    }),
};

if (process.env.NODE_ENV === "production") {
  config.headTags ||= [];
  config.headTags.push({
    tagName: "script",
    attributes: {
      src: "https://cdn.usefathom.com/script.js",
      "data-site": "RULHQVMR", // cspell:disable-line
      "data-spa": "history",
      defer: "defer",
    },
  });
}

module.exports = config;

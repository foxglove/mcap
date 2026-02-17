// @ts-check
/* eslint-env node */
/* eslint-disable filenames/match-exported */

const path = require("node:path");
const darkCodeTheme = require("prism-react-renderer/themes/dracula");
const lightCodeTheme = require("prism-react-renderer/themes/github");
const webpack = require("webpack");

const modifySvgoConfigInPlace = require("./modifySvgoConfigInPlace");

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: "MCAP",
  tagline: "MCAP file format",
  favicon: "img/favicon.png",
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
        modifySvgoConfigInPlace(config);
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
          src: "img/mcap64.webp",
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
            href: "https://foxglove.dev/chat",
            label: "Discord",
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
                label: "Discord",
                href: "https://foxglove.dev/chat",
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

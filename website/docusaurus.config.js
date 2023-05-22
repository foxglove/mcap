// @ts-check

const lightCodeTheme = require("prism-react-renderer/themes/github");
const darkCodeTheme = require("prism-react-renderer/themes/dracula");

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
      // image: "img/docusaurus-social-card.jpg",
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
                label: "MCAP Guides",
                to: "/guides",
              },
              {
                label: "MCAP API Reference",
                to: "/reference",
              },
              {
                label: "MCAP Specification",
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
                label: "Foxglove Data Platform",
                href: "https://foxglove.dev/data-platform",
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
    }),
};

if (process.env.NODE_ENV === "production") {
  config.headTags ||= [];
  config.headTags.push({
    tagName: "script",
    attributes: {
      src: "https://cdn.usefathom.com/script.js",
      "data-site": "RULHQVMR", // cspell:disable-line
      defer: "defer",
    },
  });
}

module.exports = config;

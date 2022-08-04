// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'MCAP',
  tagline: 'a serialization-agnostic container file format for pub/sub',
  url: 'https://mcap.dev',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon32.png',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'foxglove', // Usually your GitHub org/user name.
  projectName: 'mcap', // Usually your repo name.
  deploymentBranch: 'gh-pages',
  trailingSlash: false,

  // Even if you don't use internalization, you can use this field to set useful
  // metadata like html lang. For example, if your site is Chinese, you may want
  // to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/foxglove/mcap/tree/main/packages/create-docusaurus/templates/shared/',
        },
        blog: {
          showReadingTime: true,
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/foxglove/mcap/tree/main/packages/create-docusaurus/templates/shared/',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'MCAP',
        logo: {
          alt: 'MCAP logo',
          src: 'img/mcap.png',
        },
        items: [
          {
            type: 'doc',
            docId: 'what-is-mcap',
            position: 'left',
            label: 'Documentation',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Overview',
                to: '/docs/what-is-mcap',
              },
              {
                label: 'Guides',
                to: '/docs/Guides',
              },
              {
                label: 'API Documentation',
                to: '/docs/API',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'Slack',
                href: 'https://foxglovedev.slack.com'
              },
              {
                label: 'Twitter',
                href: 'https://twitter.com/foxglove',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'Foxglove',
                href: 'https://foxglove.dev',
              },
              {
                label: 'GitHub',
                href: 'https://github.com/foxglove/mcap',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Foxglove. Built with Docusaurus.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),
};

module.exports = config;

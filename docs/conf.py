# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

project = "mcap"
html_show_copyright = False
html_logo = "website/mcap.png"
html_favicon = "website/favicon32.png"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "m2r2",
]

html_theme_options = {
    "github_user": "foxglove",
    "github_repo": "mcap",
    "sidebar_collapse": False,
}

# Configuration file for the Sphinx documentation builder.


# -- Project information -----------------------------------------------------

project = "pylivy"
copyright = "2018-2020, Andrew Crozier"
author = "Andrew Crozier"


# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx_autodoc_typehints",
]

# Order class method documentation according to their order in source code
autodoc_member_order = "bysource"


# -- Options for HTML output -------------------------------------------------

html_theme = "alabaster"

html_theme_options = {
    "description": "Use Apache Livy from Python.",
    "github_user": "acroz",
    "github_repo": "pylivy",
}

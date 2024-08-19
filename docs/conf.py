from sphinxawesome_theme import ThemeOptions
from sphinxawesome_theme.postprocess import Icons

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "amlaidatatests"
copyright = "2024, Groundtruth AI Ltd"
author = "Groundtruth AI Ltd"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.napoleon",
    "sphinx.ext.autodoc",
    "sphinx_toolbox.confval",
    "sphinxarg.ext",
    "sphinx.ext.autosectionlabel",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinxawesome_theme"
html_static_path = ["_static"]
html_permalinks_icon = Icons.permalinks_icon
html_context = {
    "mode": "production",
    "feedback_url": "https://github.com/kai687/sphinxawesome-theme/issues/new?title=Feedback",
}

rst_prolog = """
.. include:: <s5defs.txt>

"""
html_css_files = ["css/s4defs-roles.css"]

theme_options = ThemeOptions(
    show_prev_next=True,
    awesome_external_links=True,
    main_nav_links={"Docs": "/index", "About": "/about"},
    extra_header_link_icons={},
)

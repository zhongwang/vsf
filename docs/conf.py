"""Sphinx configuration."""
project = "VariantSparseFormat"
author = "Zhong Wang"
copyright = "2023, Zhong Wang"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_click",
    "myst_parser",
]
autodoc_typehints = "description"
html_theme = "furo"

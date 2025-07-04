# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# some additions from ivmfnal's metacat
# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('..'))
sys.path.insert(0, os.path.abspath('../../src'))
sys.path.insert(0, os.path.abspath('../config'))
sys.path.insert(0, os.path.abspath('../../src/merge_utils'))
sys.path.insert(0, os.path.abspath('../..'))

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here.
import pathlib
#top = pathlib.Path(__file__).parents[2].resolve().as_posix()
#print("top",top)
#py = os.path.join(top,"python")
#sys.path.insert(0,top)
#sys.path.insert(0,py)
#sys.path.insert(0,os.path.join(top,"tests"))



print ("PATH",sys.path)

# -- Project information -----------------------------------------------------

project = 'merge-utils'
copyright = '2025, Fermi National Accelerator Laboratory'
author = 'Heidi Schellman and Ethan Muldoon'

# The full version, including alpha/beta/rc tags
release = '0.0'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.napoleon',
    'sphinx.ext.autosectionlabel',
    'sphinx.ext.viewcode',
    'sphinx.ext.inheritance_diagram',
    'myst_parser',
    'sphinxcontrib.programoutput'
    #'sphinx_sitemap',  had to back it out as not available
    #'sphinxcontrib.programoutput',  # not available to github
]

autoclass_content = "both"  # from ivmfnal

html_baseurl = 'https://hschellman.github.io/merge-utils-test/'

autosectionlabel_prefix_document = True

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

autodoc_mock_imports = ["metacat","samweb_client","data_dispatcher","rucio"]
# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
#html_theme = 'furo'
html_theme = 'nature'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
#html_static_path = ['_static']

html_static_path = []

# Configuration file for the Sphinx documentation builder.
# Atrum Volentia — Hegseth, Iran & the Second Strike

# -- Project information -------------------------------------------------------

project = "Atrum Volentia — Hegseth, Iran & the Second Strike"
author = "Matt Gerhold"
copyright = "2026, Matt Gerhold"
release = "1.0"

# -- General configuration -----------------------------------------------------

import os
import shutil

extensions = [
    "myst_parser",
    "sphinxcontrib.mermaid",
    "sphinxcontrib.confluencebuilder",
]

myst_enable_extensions = ["colon_fence", "deflist", "tasklist"]
source_suffix = {".md": "markdown"}

# Treat [[label]](url) as valid inline markup; suppress warnings from
# the double-bracket citation style written by rapporteur_docs.py.
myst_all_links_external = True

exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
    "raw",
    "logs",
    "generate_*.py",
    "manifest.json",
    "plan_*.md",
    "REFLECTION.md",
    "conf.py",
]

# -- HTML output ---------------------------------------------------------------

html_theme = "sphinx_rtd_theme"
html_title = "Atrum Volentia — Hegseth, Iran & the Second Strike"

# -- Mermaid settings ----------------------------------------------------------
mermaid_cmd = shutil.which("mmdc") or os.path.expanduser("~/.local/node_modules/.bin/mmdc")
mermaid_cmd_shell = "False"
mermaid_output_format = "png"
mermaid_params = [
    "-p", os.path.join(os.path.dirname(__file__), "puppeteer-config.json"),
    "-w", "1200",
    "-f",
]

# -- LaTeX / PDF output --------------------------------------------------------
# Matches Kaliya/Lamia/BBOA styling exactly.

latex_engine = "pdflatex"

latex_elements = {
    "papersize": "a4paper",
    "pointsize": "11pt",
    "preamble": r"""
\usepackage{graphicx}
\setkeys{Gin}{width=\linewidth,height=0.85\textheight,keepaspectratio=true}
\usepackage{float}
\usepackage{booktabs}
\usepackage{caption}
\captionsetup{font=small,labelfont=bf,skip=4pt}
""",
    "figure_align": "H",
    "geometry": r"\usepackage[a4paper,top=2.5cm,bottom=2.5cm,left=2.5cm,right=2.5cm]{geometry}",
}

latex_documents = [
    (
        "index",
        "atrum-volentia.tex",
        "Atrum Volentia — Hegseth, Iran \\& the Second Strike",
        "Matt Gerhold",
        "manual",
    ),
]

# -- Confluence output ---------------------------------------------------------

confluence_publish = False
confluence_server_url = ""
confluence_space_key = ""
confluence_parent_page = "Atrum Volentia"
confluence_page_hierarchy = True
confluence_publish_prefix = ""

# Configuration file for the Sphinx documentation builder.
# Rapporteur CLI — Unified Document Publishing Pipeline

# -- Project information -------------------------------------------------------

project = "Rapporteur CLI — Unified Document Publishing Pipeline"
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
myst_all_links_external = True

exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
    "conf.py",
]

# -- HTML output ---------------------------------------------------------------

html_theme = "sphinx_rtd_theme"
html_title = "Rapporteur CLI — Unified Document Publishing Pipeline"

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
# Matches Kaliya/Lamia/BBOA/AtrumVolentia styling exactly.

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
        "rapporteur-cli.tex",
        "Rapporteur CLI — Unified Document Publishing Pipeline",
        "Matt Gerhold",
        "manual",
    ),
]

# -- Confluence output ---------------------------------------------------------

confluence_publish = False
confluence_server_url = ""
confluence_space_key = ""
confluence_parent_page = "Rapporteur CLI"
confluence_page_hierarchy = True
confluence_publish_prefix = ""

# Configuration file for the Sphinx documentation builder.
# Atrum Volentia — Hegseth, Iran & the Second Strike

# -- Project information -------------------------------------------------------

project = "Atrum Volentia — Hegseth, Iran & the Second Strike"
author = "Matt Gerhold"
copyright = "2026, Matt Gerhold"
release = "1.0"

# -- General configuration -----------------------------------------------------

extensions = [
    "myst_parser",
    "sphinxcontrib.confluencebuilder",
]

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "tasklist",
    "attrs_inline",
]

# Treat [[label]](url) as valid inline markup; suppress warnings from
# the double-bracket citation style written by rapporteur_docs.py.
myst_all_links_external = True

templates_path = ["_templates"]
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
html_static_path = ["_static"]
html_title = "Atrum Volentia — Hegseth, Iran & the Second Strike"

html_theme_options = {
    "navigation_depth": 4,
    "titles_only": False,
    "collapse_navigation": False,
}

# -- LaTeX / PDF output --------------------------------------------------------

latex_engine = "pdflatex"

latex_elements = {
    "papersize": "a4paper",
    "pointsize": "11pt",
    "sphinxsetup": "TitleColor={rgb}{0,0,0}, InnerLinkColor={rgb}{0,0,0}, OuterLinkColor={rgb}{0,0,0}",
    "preamble": r"""
\usepackage[scaled]{helvet}
\renewcommand{\familydefault}{\sfdefault}
\usepackage{inconsolata}
\usepackage{setspace}\setstretch{1.4}
\usepackage[top=2.5cm,bottom=2.5cm,left=2.8cm,right=2.8cm]{geometry}
\usepackage{xcolor}
\setcounter{secnumdepth}{3}
\usepackage{titlesec}
\titleformat{\chapter}[display]{\normalfont\fontsize{18}{22}\bfseries\color{black}}{\chaptertitlename\ \thechapter}{0pt}{}
\titlespacing*{\chapter}{0pt}{10pt}{12pt}
\titleformat{\section}{\normalfont\large\bfseries\color{black}}{\thesection}{1em}{}
\titleformat{\subsection}{\normalfont\normalsize\bfseries\color{black}}{\thesubsection}{1em}{}
\titleformat{\subsubsection}{\normalfont\normalsize\itshape\color{black}}{\thesubsubsection}{1em}{}
""",
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

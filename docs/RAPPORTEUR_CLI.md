# Rapporteur CLI — Unified Document Publishing Pipeline

Chat sessions to published documents in one command.

## Quick Start

```bash
# Full pipeline — export, parse, generate chapters, analyse threats, build PDF:
python -m src.components.publisher.rapporteur_cli docs/AtrumVolentia/document.yaml --all

# Preview what would happen (no files written):
python -m src.components.publisher.rapporteur_cli docs/AtrumVolentia/document.yaml --all --dry-run
```

## Usage

```
rapporteur_cli [-h] [--export] [--parse] [--chapters] [--kaliya] [--format]
               [--conclude] [--edit] [--cite] [--introduce] [--eo-link]
               [--publish] [--all] [--force] [--dry-run] [--build-dir DIR]
               [--frontend-url URL] [-v] config
```

**Positional:**

| Argument | Description |
|---|---|
| `config` | Path to `document.yaml` configuration file |

**Stages (combine freely):**

| Flag | Stage | Description |
|---|---|---|
| `--export` | 1 | Pull conversations from frontend API (or copy static file) |
| `--parse` | 2 | Classify turns, build citation map, generate footnotes |
| `--chapters` | 3 | Render session Markdown with per-chapter references |
| `--kaliya` | 4 | Match threat clusters to Kaliya analyses, inject sections |
| `--format` | 5 | LLM-based style formatting (headings, seriation, tables) |
| `--conclude` | 5b | LLM-generated conclusion chapter (draft + reflect) |
| `--edit` | 6 | LLM-based editorial review (review + apply) |
| `--cite` | 7 | Resolve citation tokens + EO-specific APA formatting |
| `--introduce` | 8 | LLM-generated introduction chapter (draft + reflect) |
| `--eo-link` | 10 | EO precedence diagrams + chapter enrichment |
| `--publish` | 9 | Build PDF via Sphinx + xelatex |
| `--all` | 1–10 | Run all stages sequentially |

**Options:**

| Flag | Description |
|---|---|
| `--dry-run` | Preview actions without writing files |
| `--force` | Re-run stages even if already completed (override idempotency guards) |
| `--build-dir DIR` | Override build directory from config |
| `--frontend-url URL` | Override frontend API URL from config |
| `--edit-chapter N` | Edit a specific chapter by number |
| `--edit-instructions TEXT` | Custom editorial instructions for edit stage |
| `--no-format` | Skip format stage in `--all` pipeline |
| `--no-conclude` | Skip conclude stage in `--all` pipeline |
| `-v`, `--verbose` | Enable debug logging |

## Examples

```bash
# Export conversations from the live frontend:
python -m src.components.publisher.rapporteur_cli config.yaml --export

# Export + parse + build chapters:
python -m src.components.publisher.rapporteur_cli config.yaml --export --parse --chapters

# Build chapters + run Kaliya analysis + generate PDF:
python -m src.components.publisher.rapporteur_cli config.yaml --chapters --kaliya --publish

# Full LLM pipeline (format, conclude, edit, cite, introduce):
python -m src.components.publisher.rapporteur_cli config.yaml --format --conclude --edit --cite --introduce --publish

# Generate EO precedence diagrams and publish:
python -m src.components.publisher.rapporteur_cli config.yaml --eo-link --publish

# Re-run a stage that was already completed:
python -m src.components.publisher.rapporteur_cli config.yaml --cite --force

# Use a static export file (offline build):
# Set export.static_file in document.yaml, then:
python -m src.components.publisher.rapporteur_cli config.yaml --all

# Override build directory:
python -m src.components.publisher.rapporteur_cli config.yaml --all --build-dir /tmp/test_build

# Verbose output for debugging:
python -m src.components.publisher.rapporteur_cli config.yaml --all -v
```

## Pipeline Stages

### Stage 1: Export (`--export`)

Pulls conversations from the frontend API at `export.frontend_url` or copies a static JSON file specified by `export.static_file`.

**Output:** `{build_dir}/raw/conversations_{timestamp}.json` + `conversations.json` stable pointer.

### Stage 2: Parse (`--parse`)

Classifies conversation turns into chapter-worthy and footnote-worthy. Builds a unified citation map across all sessions.

**Output:** `{build_dir}/parsed/session_NNN.json`, `footnotes.json`, `unified_citations.json`.

### Stage 3: Chapters (`--chapters`)

Renders each session into a chapter Markdown file with:
- Blockquote abstract
- Post-processed response text with resolved inline citations
- Top sources summary
- Enrichment sections (BBOA media analysis)
- Unified references at chapter end
- Threat cluster detection and extraction

**Output:** `{build_dir}/chapters/*.md`, `index.md`, `manifest.json`, `{build_dir}/parsed/threat_clusters.json`.

### Stage 4: Kaliya (`--kaliya`)

Matches detected threat clusters against cached Kaliya analyses by URL overlap. Converts figure images to JPEG (resizes >1200px). Renders Kaliya narrative sections with inline figures and injects them into the corresponding chapter files.

**Requires:** `--chapters` to have run first (needs `threat_clusters.json`).

**Output:** `{build_dir}/kaliya/images/*.jpeg`, updated chapter files with injected Kaliya sections.

### Stage 5: Publish (`--publish`)

Builds PDF from chapter Markdown files via Sphinx + xelatex. Generates a temporary `_sphinx_src/` directory with cleaned chapter copies, `conf.py`, and `index.md` toctree.

**Build pipeline:**
1. Copies chapters to `_sphinx_src/` with cleaning (strip footers, blockquotes, metadata)
2. Strips mermaid blocks with placeholder note (pre-render deferred)
3. Generates `conf.py` (xelatex engine, fncychap, MyST parser) and `index.md` (natural chapter ordering)
4. Runs `sphinx-build -M latexpdf`
5. Copies PDF to build root, updates manifest

**LaTeX Styling:**
- A4 paper, 11pt font, 2.5cm margins
- xelatex with TeX Gyre Termes/Heros/Cursor fonts
- fncychap Bjarne chapter decoration
- Professional image sizing (constrained to line width and 85% text height)
- Enhanced table formatting (booktabs package)
- Professional caption styling (small font, bold labels)

**Requires:** `--chapters` to have run first. Other stages optional.

**Output:** `{build_dir}/{project_slug}.pdf`.

### Stage 10: EO Link (`--eo-link`)

Generates EO precedence diagrams (Mermaid `graph LR`) showing relationships between Executive Orders. Creates per-chapter diagrams, a unified view, and focused dependency subgraphs. Resolves EO names from 5 sources (cache, implementation data, index files, chapter text, hardcoded fallbacks). Enriches chapters with implementation tables.

**Requires:** `--chapters` to have run first (needs chapter files with EO references).

**Output:** `{build_dir}/chapters/diagrams/*.md`, updated chapter files with embedded mermaid blocks.

## Configuration: `document.yaml`

```yaml
# --- Project metadata (appears on title page and PDF metadata) ---
project:
  title: "Document Title"
  author: "Author Name"
  date: "March 2026"

# --- Paths ---
paths:
  build_dir: "/path/to/build/output"          # All stage outputs go here
  image_cache: "/path/to/image/cache"          # Shared image cache
  kaliya_cache: "/path/to/kaliya/cache"        # Kaliya analysis results

# --- Stage 1: Export ---
export:
  frontend_url: "http://localhost:5001"        # Live API endpoint
  # static_file: "path/to/export.json"         # Uncomment for offline builds

# --- Session filtering ---
sessions:
  # include: [0, 1, 2, 3]                      # Whitelist specific sessions
  # If omitted: all sessions with retrieved_docs > 0 become chapters

# --- Stage 3: Chapter rendering ---
render:
  top_sources_n: 10                            # Top sources per section
  abstract_max_words: 220                      # Abstract length limit
  postprocess: true                            # Enable bullet→heading promotion
  # chapters:                                  # Custom chapter grouping
  #   - title: "Policy Analysis"
  #     sessions: [0, 1, 2]
  #   - title: "Media Response"
  #     sessions: [3, 4]

# --- Stage 4: Kaliya analysis ---
kaliya:
  bootstrap_servers: "192.168.1.3:30094"       # Kafka brokers (for new analyses)
  topic: "kaliya-requests"                     # Kafka topic
  output_path: "/path/to/kaliya/cache"         # Where results are written
  supplementary_file: "path/to/context.json"   # Additional context for Kaliya
  analysis_window: "2025-01-01 to 2026-03-16"  # Time range filter
  poll_interval_s: 15                          # Poll interval for new results
  poll_timeout_s: 600                          # Timeout for new results

# --- Stage 5: Publish ---
publish:
  formats: ["pdf"]                             # Output formats
  pdf_engine: "pdflatex"                       # LaTeX engine
  confluence:                                  # Optional Confluence publishing
    server_url: ""
    space_key: ""
    parent_page: ""
```

### Path Resolution

Relative paths in `document.yaml` are resolved against the YAML file's directory. Absolute paths are used as-is.

## Build Output Structure

```
{build_dir}/
├── raw/
│   ├── conversations_{timestamp}.json     # Timestamped export
│   └── conversations.json                 # Stable pointer (symlink or copy)
├── parsed/
│   ├── session_000.json ... session_NNN.json
│   ├── footnotes.json
│   ├── unified_citations.json
│   ├── eo_title_index.json                # EO number → name/URL index
│   └── threat_clusters.json               # Detected threat clusters
├── chapters/
│   ├── 00-introduction.md                 # LLM-generated introduction
│   ├── 01a-part-1-introduction.md         # Section introductions
│   ├── 01-chapter-slug.md ... NN-chapter-slug.md
│   ├── NNz-part-N-conclusion.md           # Section conclusions
│   ├── 99-references.md                   # Consolidated references
│   ├── diagrams/                          # EO precedence diagrams
│   │   ├── NN_eo_XXXXX.md                 # Per-chapter diagrams
│   │   ├── unified_precedence.md          # All-EO unified diagram
│   │   └── focused_XXXXX.md               # Focused dependency subgraphs
│   ├── index.md                           # Table of contents
│   └── manifest.json                      # Chapter metadata + build info
├── kaliya/
│   └── images/
│       └── {request_id}_{image_id}.jpeg   # Converted figure images
├── _sphinx_src/                           # Temporary Sphinx build directory
│   ├── conf.py                            # Generated Sphinx config
│   ├── index.md                           # Generated toctree
│   └── *.md                               # Cleaned chapter copies
└── {project_slug}.pdf                     # Final PDF output
```

## Dependencies

| Dependency | Purpose | Required By |
|---|---|---|
| Python 3.12+ | Runtime | All |
| `requests` | Frontend API calls | Stage 1 |
| `PyYAML` | Config parsing | All |
| `Pillow` (PIL) | Image conversion (.webp/.avif → .jpeg) | Stage 4 |
| `Sphinx` (9.x) | Documentation build engine | Stage 9 |
| `myst-parser` (5.x) | MyST Markdown parser for Sphinx | Stage 9 |
| `xelatex` (TeX Live) | PDF rendering engine | Stage 9 |
| `mmdc` (mermaid-cli 11.x) | Mermaid diagram rendering | Stage 10 (optional) |
| `pdfinfo` (poppler-utils) | PDF page count extraction | Stage 9 (optional) |
| `redis` | LLM response caching (947x–1014x speedup) | Stages 5, 5b, 6, 8 |

## Troubleshooting

### pandoc not found

```bash
# Ubuntu/Debian:
sudo apt install pandoc

# Verify:
pandoc --version
```

### pdflatex / xelatex not found

```bash
# Ubuntu/Debian:
sudo apt install texlive-latex-base texlive-latex-recommended texlive-latex-extra texlive-xetex

# Verify:
xelatex --version
```

### Sphinx not found

```bash
pip install 'sphinx>=7.2.0' 'myst-parser>=2.0.0' 'sphinx-rtd-theme>=2.0.0' 'sphinxcontrib-mermaid>=0.9.2'
```

### Pillow can't open .avif files

```bash
pip install 'Pillow>=11.0'  # AVIF support built-in from 11.x
```

### NFS mount: symlink creation fails

The CLI handles this automatically — falls back to `shutil.copy2` when symlink fails with EPERM. No action needed.

### Export stage: connection refused

The frontend API must be running at `export.frontend_url`. Alternatively, use `export.static_file` to work offline with a previously saved export.

### Kaliya stage: "No threat_clusters.json found"

Run `--chapters` first. The Kaliya stage needs threat cluster data extracted by the Chapter stage.

### PDF build: LaTeX errors or "Unknown alias"

If the Sphinx + xelatex build fails, check for:
- Unescaped `&` characters in titles (auto-escaped by `_build_conf_py`)
- Unclosed mermaid fences (`` ```mermaid `` without closing `` ``` ``)
- Non-zero latexmk exit — the build checks for PDF existence before aborting; warnings are non-fatal if PDF was produced

## Running Tests

```bash
# Run the pipeline unit tests:
python -m pytest tests/test_pipeline_stages.py -v

# Run EO diagram tests:
python -m pytest tests/test_eo_diagrams.py -v

# Run the integration test suite:
python -m pytest tests/rapporteur_cli_tests.py -v

# Run with timeout (default 300s per test):
python -m pytest tests/test_pipeline_stages.py -v --timeout=300
```

The test suite validates:
- **Pipeline stages** — 54 unit tests covering sort, references, APA formatting, mermaid stripping, diagram captions, and config models
- **EO diagrams** — per-chapter, unified, and focused diagram generation with name resolution
- **Integration** — end-to-end pipeline (static export → PDF), with and without Kaliya
- **Chapter structure** — single `## References` per chapter, no orphaned sub-refs
- **Kaliya cache** — report.json, narrative.md, figures.json integrity

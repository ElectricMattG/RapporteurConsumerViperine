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
rapporteur_cli [-h] [--export] [--parse] [--chapters] [--kaliya] [--publish]
               [--all] [--dry-run] [--build-dir DIR] [--frontend-url URL] [-v]
               config
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
| `--publish` | 5 | Build PDF via pandoc + pdflatex |
| `--all` | 1–5 | Run all stages sequentially |

**Options:**

| Flag | Description |
|---|---|
| `--dry-run` | Preview actions without writing files |
| `--build-dir DIR` | Override build directory from config |
| `--frontend-url URL` | Override frontend API URL from config |
| `-v`, `--verbose` | Enable debug logging |

## Examples

```bash
# Export conversations from the live frontend:
python -m src.components.publisher.rapporteur_cli config.yaml --export

# Export + parse + build chapters:
python -m src.components.publisher.rapporteur_cli config.yaml --export --parse --chapters

# Build chapters + run Kaliya analysis + generate PDF:
python -m src.components.publisher.rapporteur_cli config.yaml --chapters --kaliya --publish

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

Concatenates chapter Markdowns (with `\newpage` separators), prepends YAML front matter with LaTeX styling, and invokes pandoc with pdflatex.

**LaTeX Styling:**
- A4 paper, 11pt font, 2.5cm margins
- Professional image sizing (constrained to line width and 85% text height, aspect ratio preserved)
- Enhanced table formatting (booktabs package)
- Professional caption styling (small font, bold labels)
- Float package for precise figure placement

**Requires:** `--chapters` to have run first. `--kaliya` is optional (images will be embedded if present).

**Output:** `{build_dir}/{project_slug}.pdf`, `{build_dir}/{project_slug}_combined.md`.

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
│   └── threat_clusters.json               # Detected threat clusters
├── chapters/
│   ├── 01-chapter-slug.md ... NN-chapter-slug.md
│   ├── index.md                           # Table of contents
│   └── manifest.json                      # Chapter metadata + build info
├── kaliya/
│   └── images/
│       └── {request_id}_{image_id}.jpeg   # Converted figure images
├── {project_slug}_combined.md             # Concatenated chapters
└── {project_slug}.pdf                     # Final PDF output
```

## Dependencies

| Dependency | Purpose | Required By |
|---|---|---|
| Python 3.12+ | Runtime | All |
| `requests` | Frontend API calls | Stage 1 |
| `PyYAML` | Config parsing | All |
| `Pillow` (PIL) | Image conversion (.webp/.avif → .jpeg) | Stage 4 |
| `pandoc` (3.x) | Markdown → PDF conversion | Stage 5 |
| `pdflatex` (TeX Live) | PDF rendering engine | Stage 5 |
| `pdfinfo` (poppler-utils) | PDF page count extraction | Stage 5 (optional) |

## Troubleshooting

### pandoc not found

```bash
# Ubuntu/Debian:
sudo apt install pandoc

# Verify:
pandoc --version
```

### pdflatex not found

```bash
# Ubuntu/Debian:
sudo apt install texlive-latex-base texlive-latex-recommended texlive-latex-extra

# Verify:
pdflatex --version
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

### PDF build: "Unknown alias" error

This occurs when pandoc misinterprets `---` separators as YAML metadata boundaries. The Publish stage automatically strips chapter footers to prevent this. If it persists, check for raw `---` lines in chapter content.

## Running Tests

```bash
# Run the integration test suite:
python -m pytest tests/rapporteur_cli_tests.py -v

# Run with timeout (default 300s per test):
python -m pytest tests/rapporteur_cli_tests.py -v --timeout=300
```

The test suite validates:
- **6.1** End-to-end pipeline (static export → PDF), with and without Kaliya
- **6.2** Chapter reference structure (single `## References` per chapter, no orphaned sub-refs)
- **6.3** Kaliya cache structure (report.json, narrative.md, figures.json integrity)

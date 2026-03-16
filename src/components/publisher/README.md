# Publisher — Rapporteur CLI

Turns chat conversations into published PDF documents via a 5-stage pipeline.

## Usage

```bash
# Full pipeline (export → parse → chapters → kaliya → PDF):
python -m src.components.publisher.rapporteur_cli docs/AtrumVolentia/document.yaml --all

# Subset of stages:
python -m src.components.publisher.rapporteur_cli docs/AtrumVolentia/document.yaml --chapters --publish

# Dry run (preview, no files written):
python -m src.components.publisher.rapporteur_cli docs/AtrumVolentia/document.yaml --all --dry-run
```

## Stages

| # | Flag | What it does |
|---|---|---|
| 1 | `--export` | Pull conversations from frontend API or copy a static JSON file |
| 2 | `--parse` | Classify turns, build unified citation map, generate footnotes |
| 3 | `--chapters` | Render Markdown chapters with abstracts, citations, references |
| 4 | `--kaliya` | Match threat clusters to cached Kaliya analyses, inject figures |
| 5 | `--publish` | Concatenate chapters → pandoc + pdflatex → PDF |

Use `--all` to run 1–5 in sequence. Stages are idempotent — rerunning overwrites cleanly.

## Configuration

Everything is driven by a single `document.yaml`. Minimal example:

```yaml
project:
  title: "My Document"
  author: "Author"
  date: "March 2026"

paths:
  build_dir: "/path/to/build"

export:
  static_file: "conversations_export.json"   # or frontend_url for live API

publish:
  formats: ["pdf"]
  pdf_engine: "pdflatex"
```

See [docs/RAPPORTEUR_CLI.md](../../../docs/RAPPORTEUR_CLI.md) for the full schema and troubleshooting guide.

## Files

| File | Lines | Purpose |
|---|---|---|
| `rapporteur_cli.py` | ~1,770 | CLI entry point + 5 stage classes |
| `rapporteur_docs.py` | — | Document rendering library (citations, references, enrichment) |
| `../../models/publisher.py` | 183 | 10 config dataclasses + `PipelineConfig.from_yaml()` |

## Build Output

```
{build_dir}/
├── raw/conversations.json       # Stage 1
├── parsed/session_*.json        # Stage 2
├── chapters/
│   ├── 01-chapter-slug.md       # Numbered with dashes (01-, 02-...)
│   ├── 02-another-chapter.md
│   ├── index.md
│   └── manifest.json
├── kaliya/images/*.jpeg         # Stage 4
└── {slug}.pdf                   # Stage 5
```

## Tests

```bash
python -m pytest tests/rapporteur_cli_tests.py -v
```

26 integration tests covering end-to-end pipeline, reference validation, and Kaliya cache structure.

## Requirements

- Python 3.12+, PyYAML, requests, Pillow 11+
- pandoc 3.x + pdflatex (TeX Live) for PDF output

# The Rapporteur CLI

## From Conversations to Publications

The Rapporteur CLI is a unified document-publishing pipeline that transforms raw chat-based research sessions into publication-ready analytical documents. It takes unstructured LLM conversations — complete with source citations, threat intelligence, executive order analysis, and media assessments — and produces structured, editorially reviewed documents with professional typography and consistent citation formatting.

The pipeline addresses a fundamental challenge in intelligence analysis: how to convert interactive research dialogues into authoritative publications that maintain analytical rigour, proper source attribution, and consistent editorial standards across dozens of chapters and hundreds of citations.

## What the Pipeline Produces

A single invocation of `rapporteur_cli` can:

- **Export** conversations from a live frontend API or static JSON archive
- **Parse** turns into chapter-worthy content vs footnotes, building unified citation maps
- **Generate** structured chapters with abstracts, resolved inline citations, and threat cluster detection
- **Integrate** Kaliya threat intelligence analyses with converted figure images
- **Link** official government communications to the Executive Order legal framework via a 7-phase EO linking sub-pipeline
- **Format** chapters against editorial style rules using LLM-based reformatting
- **Generate** conclusion and introduction chapters via two-call LLM synthesis
- **Review** content through LLM-driven editorial review with structured edit logs
- **Resolve** citation tokens to APA-formatted references with EO link normalisation
- **Publish** final output as professional PDF via Sphinx and LaTeX

## Quick Start

```bash
# Full pipeline — all 11 stages:
python -m src.components.publisher.rapporteur_cli docs/AtrumVolentia/document.yaml --all

# Preview without writing files:
python -m src.components.publisher.rapporteur_cli docs/AtrumVolentia/document.yaml --all --dry-run

# Run specific stages:
python -m src.components.publisher.rapporteur_cli config.yaml --export --parse --chapters --publish

# Include EO linking with noise filtering:
python -m src.components.publisher.rapporteur_cli config.yaml --eo-link --exclude-eos 14165 14370
```

## Scope of This Documentation

This book documents the Rapporteur CLI as of March 2026. It covers:

- **Chapter 2** — System overview and stage inventory
- **Chapter 3** — Pipeline architecture: execution model, idempotency, cost tracking
- **Chapter 4** — Data acquisition: export and parse stages
- **Chapter 5** — Chapter generation and Kaliya threat analysis injection
- **Chapter 6** — LLM enrichment: formatting, conclusion, editorial review, introduction
- **Chapter 7** — Citation resolution and EO reference normalisation
- **Chapter 8** — EO Event Linking: the 7-phase sub-pipeline
- **Chapter 9** — Publication via Sphinx and LaTeX
- **Appendix A** — Complete `document.yaml` configuration reference
- **Appendix B** — Style guide and FLASH formatting rules
- **Appendix C** — Test suite and validation

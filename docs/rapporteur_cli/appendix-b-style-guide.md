# Appendix B — Style Guide

The style guide is a Markdown file (default: `docs/style_guide.md`) that provides rules for three pipeline stages: Format, Conclude, and Edit. Each stage extracts its own section from the file.

## FLASH Style Conventions (Format Stage)

The Format stage extracts rules from the `## STYLE CONVENTIONS` section:

- Use Markdown heading levels for hierarchy — maximum three levels: `#`, `##`, `###`.
- Do **not** add numeric prefixes to headings — numbering is handled automatically by Sphinx.
- Each chapter file must have exactly **one `#` heading** (the chapter title). All other headings use `##` or `###`.
- Favour free text over lists. Use seriation (bullet points) only where clarity requires it.
- For tables, place `Table X. HEADING` on the line before each table.
- All headings must be on their own line with a blank line after.

## Conclusion Rules (Conclude Stage)

The Conclude stage extracts rules from the `## CONCLUSION` section:

- Each prompt is a continuation of the previous response — do not restart analysis.
- Aggregate information considering:
  1. Legal framework (grounding).
  2. Events and selected media sources (with full document retrieval).
  3. Threat clustering.
  4. Narrative arcs.
  5. **Conclusions** — governments fulfil a protective function and act in the best interest of their people. Criticism is respectful and honours their efforts. Draw evidence-based conclusions about media intentions and information-domain changes.
- Include a Mermaid timeline diagram where event data is available.

## Editorial Principles (Edit Stage)

The Edit stage extracts rules from the `## EDITORIAL PRINCIPLES` section:

- The federal government performs a protective function with the interests of citizens at heart (working premise).
- Aim for cohesiveness in the overall argument:
  1. Establish clear principles and facts from media, governmental, and EO sources.
  2. Operate on observations from interim analyses.
  3. Ensure facts are logically connected and draw satisfactory conclusions.
  4. Guard against confirmation bias in observations and conclusions.
  5. Consider alternative hypotheses; test and offer explanations.
  6. Consider defensive strategy based on well-being and harmony in government efforts.
- Provide an executive summary per chapter highlighting findings clearly and lucidly.
- Keep structured edit tracking for downstream processes.

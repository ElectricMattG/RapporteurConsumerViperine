# Pipeline Architecture

## Orchestration Model

The Rapporteur pipeline is **file-mediated** — there is no shared in-memory state between stages. Each stage reads upstream artefacts from disk, produces its own outputs, and records completion status in a shared manifest. The CLI orchestrator discovers stages from a fixed registry and dispatches only the ones selected by the user.

### Stage Registry

Stages live in a single ordered list:

```python
STAGES = [
    ("export",    ExportStage),
    ("parse",     ParseStage),
    ("chapters",  ChapterStage),
    ("kaliya",    KaliyaStage),
    ("eo_link",   EOLinkStage),
    ("format",    FormatStage),
    ("conclude",  ConcludeStage),
    ("edit",      EditStage),
    ("cite",      CiteStage),
    ("introduce", IntroduceStage),
    ("publish",   PublishStage),
]
```

When the user passes stage flags (`--export --parse --chapters`), the pipeline builds a sub-list of requested stages and **re-sorts** them to match `STAGES` order. This guarantees that stages always run in canonical sequence regardless of the order flags were typed on the command line. Using `--all` selects every stage.

### Dispatch Loop

```{mermaid}
flowchart TD
    CLI["CLI parses args"] --> SEL["Select requested stages"]
    SEL --> SORT["Re-sort to STAGES order"]
    SORT --> LOOP{{"For each stage"}}
    LOOP --> INST["Instantiate stage class"]
    INST --> RUN["stage.run()"]
    RUN --> OK{Success?}
    OK -- yes --> LOOP
    OK -- no --> FAIL["Raise / log error"]
    LOOP -- done --> EXIT["Pipeline complete"]
```

Each stage class is instantiated with a `PipelineConfig` and calls `run()`. If a stage raises an exception the pipeline halts — there is no automatic retry.

## Manifest Checkpointing

All inter-stage communication flows through `{build_dir}/chapters/manifest.json`. The manifest is the pipeline's single source of truth for build progress.

### Lifecycle

1. **Created** by `ChapterStage` (Stage 3) with core metadata: project name, chapter list, generation timestamp.
2. **Extended** by each downstream stage, which appends its own block (e.g., `"kaliya": {...}`, `"format": {...}`).
3. **Queried** by stages to decide whether to skip already-completed work.

### Schema (representative)

```json
{
  "project": "rapporteur-maduro",
  "generated": "2025-11-14T03:12:44Z",
  "chapters": [
    {
      "file": "01-diplomatic-signals.md",
      "title": "Diplomatic Signals and Sanctions Architecture",
      "format_status": "done",
      "eo_link_status": "done"
    }
  ],
  "format": {
    "model": "gemini-2.5-flash",
    "total_input_tokens": 184200,
    "total_output_tokens": 42300,
    "estimated_cost_usd": 0.042
  },
  "conclude": { "model": "gpt-5.4", "..." : "..." },
  "eo_linking": { "phase_results": [  ] }
}
```

### Incremental Saves

Stages that process chapters one at a time — notably `FormatStage` and `CiteStage` — call `_save_manifest_incremental()` after each chapter. If the process crashes mid-run, already-formatted chapters retain their `"done"` markers and will be skipped on the next invocation.

## Idempotency and `--force`

Each stage checks for its own completion markers in the manifest before doing work:

```python
if entry.get("format_status") == "done" and not self.cfg.force:
    logger.info("Skipping %s (already formatted)", entry["file"])
    continue
```

When `--force` is set, all guards are bypassed and every stage re-executes from scratch. New output overwrites old files in place — there is no cleanup or rollback step.

## Dry-Run Mode

Every stage honours `--dry-run`. When active, stages log what they *would* do — `[dry-run] Would call Gemini Flash for chapter 01` — without writing files or making LLM calls. Stages still return expected path objects so downstream logic does not raise `FileNotFoundError`.

## Error Handling

| Scenario | Behaviour |
|----------|-----------|
| Missing upstream output (e.g., no `raw/conversations.json` when Parse runs) | `FileNotFoundError` — pipeline halts |
| Missing optional resource (cached Kaliya analysis, image file) | Warning logged, stage continues without that resource |
| LLM API error (rate limit, timeout) | Exception propagates — pipeline halts |
| Partial completion after crash | Manifest retains per-chapter completion markers; re-run skips completed chapters |

There is no built-in retry loop. On failure, the user re-invokes the same CLI command; idempotency guards ensure only incomplete work is repeated.

## Configuration Hierarchy

`PipelineConfig` is a composite dataclass loaded from `document.yaml`:

```{mermaid}
classDiagram
    class PipelineConfig {
        +project: ProjectConfig
        +paths: PathsConfig
        +export: ExportConfig
        +sessions: SessionsConfig
        +render: RenderConfig
        +kaliya: KaliyaConfig
        +format: FormatConfig
        +conclude: ConcludeConfig
        +introduce: IntroduceConfig
        +edit: EditConfig
        +cite: CiteConfig
        +publish_cfg: PublishConfig
        +eo_linking: EOLinkingConfig
        +force: bool
        +dry_run: bool
        +from_yaml(path) PipelineConfig
    }
    PipelineConfig *-- ProjectConfig
    PipelineConfig *-- PathsConfig
    PipelineConfig *-- ExportConfig
    PipelineConfig *-- SessionsConfig
    PipelineConfig *-- RenderConfig
    PipelineConfig *-- KaliyaConfig
    PipelineConfig *-- FormatConfig
    PipelineConfig *-- ConcludeConfig
    PipelineConfig *-- IntroduceConfig
    PipelineConfig *-- EditConfig
    PipelineConfig *-- CiteConfig
    PipelineConfig *-- PublishConfig
    PipelineConfig *-- EOLinkingConfig
```

`PipelineConfig.from_yaml(path)` deserialises the YAML, resolves all relative paths against the YAML file's directory, and applies environment-variable overrides (e.g., `RAG_SERVICE_URL`). Each sub-config groups the knobs for a single stage, keeping concerns isolated while allowing cross-stage references through the parent `PipelineConfig`.

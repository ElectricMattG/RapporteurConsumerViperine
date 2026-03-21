# LLM Enrichment

Stages 6, 7, 8, and 10 use large-language-model calls to reformat, synthesise, review, and introduce the publication. Each stage follows a shared pattern — load the manifest, check idempotency, call the LLM, write results, update the manifest — but the prompt strategy and provider differ by task.

## Stage 6 — Format

`FormatStage` applies style rules to every chapter via Gemini Flash.

```{mermaid}
flowchart TD
    CH["Chapter *.md"] --> SPLIT["Split body / references"]
    SPLIT --> PROMPT["Build prompt\n(body + style rules + table context)"]
    PROMPT --> LLM["Gemini Flash\ntemperature 0.2"]
    LLM --> CHECK{"Output ≥ 80%\nof input?"}
    CHECK -- no --> CONT["Continuation prompt\n(up to 5 passes)"]
    CONT --> LLM
    CHECK -- yes --> POST["Post-process\n(strip fences, reattach refs)"]
    POST --> OUT["Formatted chapter"]
```

### Style Rules

Rules are extracted from the `## STYLE CONVENTIONS` section of the configured style guide file. The full conventions block is included verbatim in every formatting prompt.

### Reference Sandboxing

Before sending a chapter to the model, the reference section (split at `## Top Sources` or `## References`) is removed. This reduces token cost and prevents the model from rewriting citations. After formatting, the original references are reattached.

### Continuation Logic

If the model's output is shorter than **80 %** of the input body, the stage sends a continuation prompt. Up to **5** continuation passes are attempted. If the model still truncates after 5 passes, the stage logs a warning with the completion percentage and moves on.

### Table Counter Continuity

The stage tracks the last table number across chapters so that cross-chapter table numbering is continuous (e.g., Table 4 in chapter 2 is followed by Table 5 in chapter 3).

### Provider Details

| Field | Value |
|-------|-------|
| Default model | `gemini-2.5-flash` |
| Temperature | 0.2 |
| Token estimation | input chars ÷ 3 + 20 % headroom |
| Pricing | Input $0.30 / Output $2.50 per 1M tokens (Flash) |

---

## Stage 7 — Conclude

`ConcludeStage` generates a conclusion chapter using a two-call synthesis pattern.

```{mermaid}
flowchart TD
    CHAPS["All source chapters"] --> CTX["Build document context"]
    CTX --> EO["Fetch EO grounding\n(RAG service, optional)"]
    EO --> C1["Call 1: CONCLUDE\nDraft synthesis"]
    C1 --> C2["Call 2: REFLECT\nStrengthen arguments"]
    C2 --> WRITE["NN-conclusion.md"]
```

### Two-Call Synthesis

1. **CONCLUDE** — the model receives the full document context, optional EO grounding material, and style rules. It synthesises a conclusion that ties together the publication's themes.
2. **REFLECT** — the same model receives the draft conclusion and is asked to critically review it: strengthen weak arguments, add evidence references, and tighten prose.

### EO Grounding (Optional)

If the RAG service is reachable, the stage queries `/query_metadata` against the EO index to retrieve full Executive Order documents. These are formatted as a grounding block and appended to the prompt, giving the model verified EO text to cite.

### Provider Details

| Field | Value |
|-------|-------|
| Default model | `gpt-5.4` (OpenAI Responses API) |
| Max output tokens | 16,000 |
| Temperature | 0.2 |
| Reasoning effort | Configurable (`medium` / `high`) |
| Fallback | Gemini 2.5 Pro/Flash also supported |

---

## Stage 8 — Edit

`EditStage` runs a holistic editorial review and applies targeted fixes.

```{mermaid}
flowchart TD
    ALL["All chapters"] --> R["Call 1: REVIEW\nScore + edit log (JSON)"]
    R --> PARSE["Parse edit log"]
    PARSE --> PERCH["Group edits by chapter"]
    PERCH --> A["Call 2+: APPLY\nRevise each chapter"]
    A --> LOG["editorial_edit_log.json"]
    A --> OUT["Revised chapters"]
```

### Full-Document Review

The REVIEW call assesses the entire document and returns a structured JSON edit log:

```json
{
  "document_score": {
    "stylistic_consistency": 8,
    "argument_cohesiveness": 7,
    "bias_risk": 3,
    "logical_coherence": 8
  },
  "executive_summaries": [
    { "chapter": 1, "summary": "..." }
  ],
  "edits": [
    {
      "chapter": 1,
      "section": "1.2 Sanctions Framework",
      "issue": "Unsubstantiated claim about timeline",
      "recommendation": "Add EO 14148 reference",
      "severity": "high",
      "type": "hallucinated_reference"
    }
  ]
}
```

Scores are 1–10. `bias_risk` is inverse — 1 means low risk.

### Edit Types

| Type | Meaning |
|------|---------|
| `stylistic_consistency` | Deviation from style guide |
| `logical_coherence` | Argument does not follow from evidence |
| `confirmation_bias` | One-sided framing |
| `alternative_hypothesis` | Missing counter-argument |
| `protective_function` | Unsupported causal claim |
| `argument_flow` | Poor section-to-section transition |
| `hallucinated_reference` | Reference to a non-existent source or EO |

### Targeted Chapter Mode

When `--edit-chapter` is passed, the stage reviews only chapters whose filename matches the given substring. Custom instructions (`--edit-instructions`) and a citable EO list are injected into the prompt. EO references not in the citable list are flagged as hallucinations.

### Provider Details

Same as ConcludeStage — defaults to OpenAI GPT-5.x with configurable reasoning effort.

---

## Stage 10 — Introduce

`IntroduceStage` generates an introduction chapter using the same two-call synthesis pattern as ConcludeStage.

```{mermaid}
flowchart TD
    CHAPS["Source chapters\n(skip intro/conclusion)"] --> SUM["Extract title +\nfirst 3 paragraphs each"]
    SUM --> EOG["Fetch EO grounding\n(optional)"]
    EOG --> C1["Call 1: INTRODUCE\nDraft introduction"]
    C1 --> C2["Call 2: REFLECT\nStrengthen hook & flow"]
    C2 --> WRITE["00-introduction.md"]
    WRITE --> RIDX["Rebuild index.md"]
```

### Chapter Summaries

For each source chapter (excluding any prior introduction or conclusion), the stage extracts the title and the first **3 non-heading, non-image, non-code paragraphs**. These summaries give the model enough context to describe the publication's scope without overwhelming the prompt.

### Citable EO Constraint

The introduction prompt includes a list of EO numbers that actually appear in the publication. The model is instructed to reference *only* these EOs, preventing hallucinated citations. The REFLECT call verifies compliance.

### Output Placement

The introduction is always written to `00-introduction.md` and inserted at position 0 in the manifest's chapter list. The stage rebuilds `index.md` so the introduction appears first in the table of contents.

### Provider Details

Same as ConcludeStage — defaults to OpenAI GPT-5.x.

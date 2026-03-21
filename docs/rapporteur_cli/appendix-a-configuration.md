# Appendix A — Configuration Reference

The pipeline is configured through a `document.yaml` file. `PipelineConfig.from_yaml(path)` loads the file, resolves relative paths against its directory, and applies environment-variable overrides.

## Top-Level Structure

```yaml
project:
  title: "Rapporteur — Maduro"
  author: ""
  date: ""

paths:
  build_dir: "working_dir/maduro"
  image_cache: ""
  kaliya_cache: ""

export:
  frontend_url: "http://localhost:5001"
  static_file: ""

sessions:
  include: [3, 4, 5, 7, 8]

render:
  top_sources_n: 10
  abstract_max_words: 220
  postprocess: true
  chapters:
    - title: "Diplomatic Signals"
      sessions: [3, 4]
    - title: "Sanctions Architecture"
      sessions: [5]

kaliya:
  bootstrap_servers: "192.168.1.3:30094"
  topic: "kaliya-requests"
  output_path: "/mnt/workspace_2/bboa/cache/kaliya_agenda_cache"
  supplementary_file: ""
  analysis_window: ""
  poll_interval_s: 15
  poll_timeout_s: 600
  include_images: true

format:
  enabled: false
  style_guide: "docs/style_guide.md"
  provider: "gemini"
  model: "gemini-2.5-pro"

conclude:
  enabled: false
  style_guide: "docs/style_guide.md"
  provider: "openai"
  model: "gpt-5.4"
  reasoning_effort: "high"
  include_kaliya: true
  rag_service_url: "http://localhost:8000"

introduce:
  enabled: false
  style_guide: "docs/style_guide.md"
  provider: "openai"
  model: "gpt-5.4"
  reasoning_effort: "high"
  include_kaliya: false
  rag_service_url: "http://localhost:8000"

edit:
  enabled: false
  style_guide: "docs/style_guide.md"
  provider: "openai"
  model: "gpt-5.4"
  reasoning_effort: "high"
  edit_log: true
  executive_summary: true
  target_chapter: null
  custom_instructions: null

cite:
  enabled: false
  eo_name_max_chars: 60
  resolve_web_citations: true
  apa_first_mention_title: true

publish:
  formats: ["pdf"]
  pdf_engine: "pdflatex"
  confluence:
    server_url: ""
    space_key: ""
    parent_page: ""

eo_linking:
  enabled: false
  rag_service_url: "http://localhost:8080"
  index_folders: ["x_indices_hegseth", "x_indices_trump"]
  domain_include: [".gov", ".mil", ".gov.il", "dirco.gov.za"]
  domain_exclude: []
  eo_book_dir: "/path/to/chapters"
  eo_index_folder: "x_indices_eo"
  batch_size: 150
  rag_summaries: 3
  rag_qa_pairs: 3
  analysis_provider: "gemini"
  analysis_model: "gemini-3.1-pro-preview"
  analysis_reasoning_effort: "high"
  analysis_location: "global"
  output_cache_path: "eo_implementation_cache.jsonl"
  shared_volume: "/mnt/workspace_2"
  exclude_eos: []
```

## Sub-Config Reference

### ProjectConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `title` | `str` | `"Untitled"` | Publication title |
| `author` | `str` | `""` | Author name |
| `date` | `str` | `""` | Publication date |

### PathsConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `build_dir` | `str` | `""` | Root build directory |
| `image_cache` | `str` | `""` | Path to image cache |
| `kaliya_cache` | `str` | `""` | Path to Kaliya analysis cache |

### ExportConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `frontend_url` | `str` | `"http://localhost:5001"` | Frontend API base URL |
| `static_file` | `str` | `""` | Path to static conversations JSON (bypasses API) |

### SessionsConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `include` | `list[int] \| None` | `None` | Whitelist of session indices to include as chapters |

### RenderConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `top_sources_n` | `int` | `10` | Max references per section |
| `abstract_max_words` | `int` | `220` | Abstract word limit |
| `postprocess` | `bool` | `True` | Enable post-processing on rendered text |
| `chapters` | `list[ChapterSpec]` | `[]` | Explicit chapter → session mapping |

### FormatConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `False` | Enable formatting stage |
| `style_guide` | `str` | `"docs/style_guide.md"` | Path to style guide Markdown |
| `provider` | `str` | `"gemini"` | LLM provider |
| `model` | `str` | `"gemini-2.5-pro"` | Model identifier |

### ConcludeConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `False` | Enable conclusion generation |
| `style_guide` | `str` | `"docs/style_guide.md"` | Path to style guide |
| `provider` | `str` | `"openai"` | LLM provider |
| `model` | `str` | `"gpt-5.4"` | Model identifier |
| `reasoning_effort` | `str` | `"high"` | Reasoning depth (`low`, `medium`, `high`) |
| `include_kaliya` | `bool` | `True` | Include Kaliya narratives in context |
| `rag_service_url` | `str` | `"http://localhost:8000"` | RAG service for EO grounding |

### IntroduceConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `False` | Enable introduction generation |
| `style_guide` | `str` | `"docs/style_guide.md"` | Path to style guide |
| `provider` | `str` | `"openai"` | LLM provider |
| `model` | `str` | `"gpt-5.4"` | Model identifier |
| `reasoning_effort` | `str` | `"high"` | Reasoning depth |
| `include_kaliya` | `bool` | `False` | Include Kaliya narratives |
| `rag_service_url` | `str` | `"http://localhost:8000"` | RAG service for EO grounding |

### EditConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `False` | Enable editorial review |
| `style_guide` | `str` | `"docs/style_guide.md"` | Path to style guide |
| `provider` | `str` | `"openai"` | LLM provider |
| `model` | `str` | `"gpt-5.4"` | Model identifier |
| `reasoning_effort` | `str` | `"high"` | Reasoning depth |
| `edit_log` | `bool` | `True` | Write edit log JSON |
| `executive_summary` | `bool` | `True` | Generate per-chapter summaries |
| `target_chapter` | `str \| None` | `None` | Target a specific chapter (filename substring) |
| `custom_instructions` | `str \| None` | `None` | Extra editorial instructions |

### CiteConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `False` | Enable citation resolution |
| `eo_name_max_chars` | `int` | `60` | Max title length in first-mention APA label |
| `resolve_web_citations` | `bool` | `True` | Resolve non-EO web citations |
| `apa_first_mention_title` | `bool` | `True` | Include title on first mention |

### PublishConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `formats` | `list[str]` | `["pdf"]` | Output formats |
| `pdf_engine` | `str` | `"pdflatex"` | LaTeX engine |
| `confluence` | `ConfluenceConfig` | — | Confluence publishing settings |

### EOLinkingConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `False` | Enable EO linking stage |
| `rag_service_url` | `str` | `"http://localhost:8080"` | RAG service URL |
| `index_folders` | `list[str]` | `["x_indices_hegseth", "x_indices_trump"]` | Research index directories |
| `domain_include` | `list[str]` | `[".gov", ".mil", ".gov.il", "dirco.gov.za"]` | Domain suffix whitelist |
| `domain_exclude` | `list[str]` | `[]` | Domain blocklist |
| `eo_book_dir` | `str` | — | Path to EO chapter files |
| `eo_index_folder` | `str` | `"x_indices_eo"` | EO event index directory |
| `batch_size` | `int` | `150` | Articles per batch |
| `rag_summaries` | `int` | `3` | Top-k summaries per RAG query |
| `rag_qa_pairs` | `int` | `3` | Top-k QA pairs per RAG query |
| `analysis_provider` | `str` | `"gemini"` | LLM provider for analysis |
| `analysis_model` | `str` | `"gemini-3.1-pro-preview"` | LLM model for analysis |
| `analysis_reasoning_effort` | `str` | `"high"` | Reasoning depth |
| `analysis_location` | `str` | `"global"` | API endpoint location |
| `output_cache_path` | `str` | — | Path to implementation cache JSONL |
| `shared_volume` | `str` | `"/mnt/workspace_2"` | Shared volume mount |
| `exclude_eos` | `list[str]` | `[]` | EO numbers to exclude from diagrams/tables |

## Environment Variable Overrides

| Variable | Overrides |
|----------|-----------|
| `RAG_SERVICE_URL` | `conclude.rag_service_url`, `introduce.rag_service_url`, `eo_linking.rag_service_url` |
| `REPO_DIR` | Repository root path |
| `SHARED_VOLUME` | `eo_linking.shared_volume` |

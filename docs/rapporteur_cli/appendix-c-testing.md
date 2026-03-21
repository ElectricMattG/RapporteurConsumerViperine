# Appendix C — Testing

The test suite lives in `tests/rapporteur_cli_tests.py` and covers pipeline stages through a mix of unit tests and integration tests.

## Test Coverage

| Test Class | Stage | Cases | Focus |
|-----------|-------|-------|-------|
| `TestEndToEndStaticExport` | 1–11 | 7 | Full pipeline from static file to PDF |
| `TestEndToEndWithKaliya` | 1–11 | 5 | Full pipeline with Kaliya enrichment |
| `TestChapterReferences` | 3 | 4 | Reference section structure and placement |
| `TestLiveBuildReferences` | 3 | 5 | No duplicate references, abstract and footer presence |
| `TestKaliyaCacheStructure` | 4 | 5 | Cache file integrity (report.json, figures.json, images) |
| `TestConcludeConfig` | 7 | 3 | Config defaults and YAML deserialisation |
| `TestConcludeStageUnit` | 7 | 7 | Idempotency, dry run, prompt construction |
| `TestConcludeCLIFlags` | 7 | 3 | `--conclude` / `--no-conclude` flag parsing |
| `TestConcludeStageRegistration` | 7 | 2 | Stage position in `STAGES` tuple |
| `TestEditConfig` | 8 | 3 | Config defaults and YAML deserialisation |
| `TestEditStageUnit` | 8 | 11 | Idempotency, dry run, edit log parsing, prompts |
| `TestEditCLIFlags` | 8 | 3 | `--edit` / `--no-edit` flag parsing |
| `TestEditStageRegistration` | 8 | 2 | Stage position in `STAGES` tuple |

**Total: 60+ test cases.**

## Running Tests

```bash
# All CLI tests
python -m pytest tests/rapporteur_cli_tests.py -v

# A single test class
python -m pytest tests/rapporteur_cli_tests.py::TestConcludeStageUnit -v

# With coverage
python -m pytest tests/rapporteur_cli_tests.py --cov=src.components.publisher.rapporteur_cli
```

## Key Test Patterns

### Idempotency Tests

Each stage that writes manifest markers has a test verifying that a second invocation with the same manifest skips the stage (unless `--force` is set):

```python
def test_skips_if_already_generated(self):
    manifest["conclude"] = {"status": "done"}
    stage.run()
    assert llm_client.call_count == 0
```

### Dry-Run Tests

Stages are invoked with `dry_run=True` and the test asserts that no files were written and no LLM calls were made.

### Config Round-Trip Tests

`from_yaml()` tests verify that a YAML snippet deserialises to the expected dataclass fields, and that missing sections fall back to defaults.

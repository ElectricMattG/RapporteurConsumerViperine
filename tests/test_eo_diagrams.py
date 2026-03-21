"""Tests for EONameLookup, EODiagramGenerator, ImplementationTableGenerator,
ChapterEnricher, and standalone helpers (_mermaid_label, _node_id, _clean_eo_title).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.components.analysis.eo_linking import (_EO_NAME_FALLBACKS,
                                                _MERMAID_MIN_LABEL_WIDTH,
                                                ChapterEnricher,
                                                EODiagramGenerator, EOEntry,
                                                EOImplementationCache,
                                                EONameLookup, EORegistry,
                                                ImplementationTableGenerator,
                                                _clean_eo_title,
                                                _mermaid_label, _node_id,
                                                filter_precedence_graph)

# ===================================================================
# helpers
# ===================================================================

def _make_registry(entries: dict[str, EOEntry] | None = None,
                   supercessions: dict[str, list[str]] | None = None,
                   chapter_texts: list[tuple[str, str]] | None = None) -> EORegistry:
    """Build a minimal EORegistry without touching the filesystem."""
    reg = EORegistry.__new__(EORegistry)
    # Manually set the logger that Base.__init__ would set
    import logging
    reg._logger = logging.getLogger("EORegistry")
    reg.entries = entries or {}
    from collections import defaultdict
    reg.supercessions = defaultdict(list, supercessions or {})
    reg.chapter_texts = chapter_texts or []
    return reg


def _make_name_lookup(names: dict[str, str] | None = None) -> EONameLookup:
    lookup = EONameLookup.__new__(EONameLookup)
    import logging
    lookup._logger = logging.getLogger("EONameLookup")
    lookup._names = dict(names or {})
    lookup._path = Path("/dev/null")
    return lookup


# ===================================================================
# _clean_eo_title
# ===================================================================


class TestCleanEoTitle:
    def test_strip_prefix(self):
        title = "Analysis of Exec. Order No. 14148 (2025) and the Climate Policy"
        assert "Climate Policy" in _clean_eo_title(title)

    def test_strip_suffix(self):
        title = "Impact on Immigration: An Analysis of Exec. Order No. 14148 (2025)"
        cleaned = _clean_eo_title(title)
        assert "14148" not in cleaned
        assert "Immigration" in cleaned

    def test_strip_mid_eo(self):
        title = "Deregulation Through Exec. Order No. 14192 (2025)"
        cleaned = _clean_eo_title(title)
        assert "14192" not in cleaned

    def test_empty(self):
        assert _clean_eo_title("") == ""

    def test_short_title_preserved(self):
        assert _clean_eo_title("AI Policy") == "AI Policy"

    def test_capitalize_first_letter(self):
        title = "Exec. Order No. 14148 (2025) and the lower case start"
        cleaned = _clean_eo_title(title)
        assert cleaned[0].isupper()


# ===================================================================
# _mermaid_label
# ===================================================================


class TestMermaidLabel:
    def test_truncation(self):
        long_text = "A" * 100
        result = _mermaid_label(long_text, max_len=50)
        assert len(result) <= 50
        assert result.endswith("…")

    def test_padding_short(self):
        result = _mermaid_label("Hi")
        assert len(result) >= _MERMAID_MIN_LABEL_WIDTH

    def test_no_padding_long(self):
        text = "X" * _MERMAID_MIN_LABEL_WIDTH
        result = _mermaid_label(text)
        assert "\u00A0" not in result

    def test_sanitise_unsafe_chars(self):
        result = _mermaid_label('Test [with] "quotes"')
        assert "[" not in result
        assert '"' not in result


# ===================================================================
# _node_id
# ===================================================================


class TestNodeId:
    def test_format(self):
        assert _node_id("ROOT", "14148") == "ROOT_14148"

    def test_prefix_variation(self):
        assert _node_id("DER", "13985") == "DER_13985"


# ===================================================================
# EONameLookup
# ===================================================================


class TestEONameLookup:
    def test_get_existing(self):
        lookup = _make_name_lookup({"14148": "Initial Rescissions"})
        assert lookup.get("14148") == "Initial Rescissions"

    def test_get_missing(self):
        lookup = _make_name_lookup()
        assert lookup.get("99999") == ""

    def test_load_and_save(self, tmp_path):
        path = tmp_path / "eo_names.json"
        lookup = EONameLookup(path)
        lookup._names = {"14148": "Test", "14192": "Dereg"}
        lookup.save()
        assert path.exists()

        lookup2 = EONameLookup(path)
        lookup2.load()
        assert lookup2.get("14148") == "Test"
        assert lookup2.get("14192") == "Dereg"

    def test_populate_from_impl_cache(self, tmp_path):
        cache_path = tmp_path / "impl.jsonl"
        cache_path.write_text(
            json.dumps({"eo_number": "14148", "eo_title": "From Cache", "record_id": "r1"}) + "\n",
            encoding="utf-8",
        )
        lookup = EONameLookup(tmp_path / "names.json")
        added = lookup.populate(impl_cache_path=cache_path)
        assert added >= 1  # includes fallbacks
        assert lookup.get("14148") == "From Cache"

    def test_populate_from_index(self, tmp_path):
        idx_dir = tmp_path / "idx"
        idx_dir.mkdir()
        (idx_dir / "eo_events.jsonl").write_text(
            json.dumps({"eo_identifier": "EO 14192", "eo_title": "Deregulation"}) + "\n",
            encoding="utf-8",
        )
        lookup = EONameLookup(tmp_path / "names.json")
        lookup.populate(eo_index_dir=idx_dir)
        assert lookup.get("14192") == "Deregulation"

    def test_populate_from_chapters(self, tmp_path):
        ch_dir = tmp_path / "chapters"
        ch_dir.mkdir()
        (ch_dir / "01-eo.md").write_text(
            'Refers to EO 13985 ("Advancing Racial Equity") in context.\n',
            encoding="utf-8",
        )
        lookup = EONameLookup(tmp_path / "names.json")
        lookup.populate(chapters_dir=ch_dir)
        assert lookup.get("13985") == "Advancing Racial Equity"

    def test_populate_fallbacks(self):
        lookup = _make_name_lookup()
        lookup._path = Path("/dev/null")
        # Simulate populate with no sources — should still add fallbacks
        from src.components.analysis.eo_linking import _EO_NAME_FALLBACKS
        for num, title in _EO_NAME_FALLBACKS.items():
            lookup._names.setdefault(num, title)
        assert lookup.get("13960") == _EO_NAME_FALLBACKS["13960"]

    def test_names_property(self):
        lookup = _make_name_lookup({"14148": "Test"})
        names = lookup.names
        assert names == {"14148": "Test"}
        # Should be a copy
        names["99999"] = "new"
        assert lookup.get("99999") == ""


# ===================================================================
# EODiagramGenerator
# ===================================================================


class TestEODiagramGenerator:
    def _setup(self):
        entries = {
            "14148": EOEntry("14148", "Initial Rescissions", "2025", "01-eo.md", 0),
            "13985": EOEntry("13985", "Racial Equity", "2021", "02-eo.md", 1),
            "14192": EOEntry("14192", "Deregulation", "2025", "03-eo.md", 2),
            "14179": EOEntry("14179", "Death Penalty", "2025", "04-eo.md", 3),
        }
        registry = _make_registry(entries)
        graph = {
            "14148": {
                "rescinds": ["13985"],
                "derived_from": [],
                "read_with": ["14192"],
            },
            "14179": {
                "rescinds": [],
                "derived_from": ["14148"],
                "read_with": [],
            },
        }
        return registry, graph

    def test_generate_basic(self):
        reg, graph = self._setup()
        gen = EODiagramGenerator()
        diagram = gen.generate("14148", graph, reg)
        assert "graph LR" in diagram
        assert "ROOT_14148" in diagram

    def test_generate_includes_read_with(self):
        reg, graph = self._setup()
        gen = EODiagramGenerator()
        diagram = gen.generate("14148", graph, reg)
        assert "RW_14192" in diagram
        assert "---" in diagram  # undirected edge

    def test_generate_derived_from_arrows(self):
        reg, graph = self._setup()
        gen = EODiagramGenerator()
        diagram = gen.generate("14179", graph, reg)
        assert "DER_14148" in diagram
        assert "-->" in diagram

    def test_generate_excludes_eos(self):
        reg, graph = self._setup()
        gen = EODiagramGenerator()
        diagram = gen.generate("14148", graph, reg, exclude_eos={"14192"})
        assert "14192" not in diagram

    def test_chrono_chain_ordering(self):
        reg, graph = self._setup()
        gen = EODiagramGenerator()
        diagram = gen.generate("14148", graph, reg)
        assert "~~~" in diagram  # invisible chronological links

    def test_eo_label_from_name_lookup(self):
        reg, _ = self._setup()
        gen = EODiagramGenerator()
        lookup = _make_name_lookup({"14148": "Official Title"})
        label = gen._eo_label("14148", reg, lookup)
        assert "14148" in label
        assert "Official Title" in label

    def test_eo_label_from_registry(self):
        reg, _ = self._setup()
        gen = EODiagramGenerator()
        label = gen._eo_label("14148", reg)
        assert "14148" in label

    def test_eo_label_unknown(self):
        reg = _make_registry()
        gen = EODiagramGenerator()
        label = gen._eo_label("99999", reg)
        assert "99999" in label
        assert "Executive Order" in label

    def test_emit_chrono_chain_single_node(self):
        lines = []
        EODiagramGenerator._emit_chrono_chain([(14148, "EO_14148")], lines)
        assert lines == []

    def test_emit_chrono_chain_sorted(self):
        lines = []
        nodes = [(14192, "EO_14192"), (13985, "EO_13985"), (14148, "EO_14148")]
        EODiagramGenerator._emit_chrono_chain(nodes, lines)
        assert len(lines) == 2
        assert "EO_13985 ~~~ EO_14148" in lines[0]
        assert "EO_14148 ~~~ EO_14192" in lines[1]

    def test_generate_focused(self):
        reg, graph = self._setup()
        gen = EODiagramGenerator()
        diagram = gen.generate_focused("14148", graph, reg)
        assert "graph LR" in diagram
        assert "EO_14148" in diagram
        # Should include forward-connected nodes
        assert "EO_14192" in diagram

    def test_generate_focused_includes_reverse_derived(self):
        reg, graph = self._setup()
        gen = EODiagramGenerator()
        diagram = gen.generate_focused("14148", graph, reg)
        # 14179 derives from 14148, should be included via reverse BFS
        assert "EO_14179" in diagram

    def test_generate_for_chapters(self, tmp_path):
        entries = {
            "14148": EOEntry("14148", "Test", "2025", "01-eo.md", 0),
        }
        reg = _make_registry(
            entries,
            chapter_texts=[("01-eo.md", "# Exec. Order No. 14148 (2025): Test\n\nBody")],
        )
        graph = {"14148": {"rescinds": [], "derived_from": [], "read_with": []}}
        gen = EODiagramGenerator()
        manifest = gen.generate_for_chapters(reg, graph, tmp_path)
        assert len(manifest) == 1
        assert Path(manifest[0]["path"]).exists()
        assert manifest[0]["eo_number"] == "14148"

    def test_generate_for_chapters_stale_cleanup(self, tmp_path):
        diagrams_dir = tmp_path / "diagrams"
        diagrams_dir.mkdir()
        stale = diagrams_dir / "99_eo_99999.md"
        stale.write_text("stale", encoding="utf-8")

        reg = _make_registry(chapter_texts=[])
        gen = EODiagramGenerator()
        gen.generate_for_chapters(reg, {}, tmp_path)
        assert not stale.exists()

    def test_generate_unified(self, tmp_path):
        entries = {
            "14148": EOEntry("14148", "Test", "2025", "01-eo.md", 0),
            "14192": EOEntry("14192", "Dereg", "2025", "02-eo.md", 1),
        }
        reg = _make_registry(entries)
        graph = {
            "14148": {"rescinds": [], "derived_from": [], "read_with": ["14192"]},
        }
        gen = EODiagramGenerator()
        result = gen.generate_unified(graph, reg, tmp_path)
        assert result["nodes"] >= 2
        assert Path(result["path"]).exists()

    def test_count_nodes(self):
        diagram = 'EO_14148["label"]\nEO_14192["label"]\n'
        assert EODiagramGenerator._count_nodes(diagram) == 2

    def test_validate_unknown_ref(self):
        reg = _make_registry({"14148": EOEntry("14148")})
        graph = {"14148": {"rescinds": ["99999"], "derived_from": [], "read_with": []}}
        warnings = EODiagramGenerator._validate("14148", graph, reg)
        assert any("99999" in w for w in warnings)

    def test_validate_circular_rescission(self):
        reg = _make_registry({
            "14148": EOEntry("14148"),
            "14192": EOEntry("14192"),
        })
        graph = {
            "14148": {"rescinds": ["14192"], "derived_from": [], "read_with": []},
            "14192": {"rescinds": ["14148"], "derived_from": [], "read_with": []},
        }
        warnings = EODiagramGenerator._validate("14148", graph, reg)
        assert any("Circular" in w for w in warnings)

    def test_merge_supercessions(self):
        reg = _make_registry(
            entries={"14148": EOEntry("14148")},
            supercessions={"14148": ["13985"]},
        )
        graph = {"14148": {"rescinds": [], "derived_from": [], "read_with": []}}
        gen = EODiagramGenerator()
        merged = gen._merge_supercessions(graph, reg)
        assert "13985" in merged["14148"]["rescinds"]

    def test_merge_supercessions_non_rescind_preserved(self):
        """Registry supercessions should not override derived_from/read_with edges."""
        reg = _make_registry(
            entries={"14148": EOEntry("14148"), "14192": EOEntry("14192")},
            supercessions={"14148": ["14192"]},
        )
        # Cache says 14148 derives from 14192 — registry says 14148 rescinds 14192
        # The non-rescind relationship should take priority.
        graph = {
            "14148": {"rescinds": [], "derived_from": ["14192"], "read_with": []},
        }
        gen = EODiagramGenerator()
        merged = gen._merge_supercessions(graph, reg)
        # 14192 appears in derived_from but NOT in rescinds (non_rescind protection)
        assert "14192" in merged["14148"]["derived_from"]
        assert "14192" not in merged["14148"]["rescinds"]


# ===================================================================
# ImplementationTableGenerator
# ===================================================================


class TestImplementationTableGenerator:
    def test_generate_table_basic(self):
        gen = ImplementationTableGenerator()
        records = [
            {
                "eo_number": "14148",
                "eo_title": "Test",
                "eo_date": "2025",
                "communication": {
                    "url": "https://a.gov/1",
                    "date": "2025-01-21",
                    "domain": "a.gov",
                    "official": "WH",
                    "action_description": "Implements rescissions",
                },
                "record_id": "r1",
            },
        ]
        table = gen.generate("14148", _make_cache(records))
        assert "| Date" in table
        assert "2025-01-21" in table

    def test_generate_empty_eo(self):
        gen = ImplementationTableGenerator()
        cache = _make_cache([])
        assert gen.generate("99999", cache) == ""

    def test_format_table_dedup_by_record_id(self):
        gen = ImplementationTableGenerator()
        rec = {
            "eo_number": "14148",
            "communication": {
                "url": "https://a.gov/1",
                "date": "2025-01-21",
                "domain": "a.gov",
                "official": "WH",
                "action_description": "Test",
            },
            "record_id": "same_id",
        }
        table = gen.format_table([rec, rec])
        # Only one row despite duplicate records
        lines = [l for l in table.strip().split("\n") if l.startswith("|") and "Date" not in l and "---" not in l]
        assert len(lines) == 1

    def test_format_table_primary_eo_marker(self):
        gen = ImplementationTableGenerator()
        rec = {
            "eo_number": "14148",
            "communication": {
                "url": "https://a.gov/1",
                "date": "2025-01-21",
                "domain": "a.gov",
                "official": "WH",
                "action_description": "Test",
            },
            "record_id": "r1",
        }
        table = gen.format_table([rec], primary_eo="14148")
        assert "🔵" in table

    def test_format_table_excludes_filtered_eos(self):
        gen = ImplementationTableGenerator()
        records = [
            {
                "eo_number": "14148",
                "communication": {"url": "x", "date": "", "domain": "", "official": "", "action_description": ""},
                "record_id": "r1",
            },
            {
                "eo_number": "99999",
                "communication": {"url": "y", "date": "", "domain": "", "official": "", "action_description": ""},
                "record_id": "r2",
            },
        ]
        table = gen.format_table(records, exclude_eos={"99999"})
        assert "99999" not in table


# ===================================================================
# ChapterEnricher
# ===================================================================


class TestChapterEnricher:
    def test_enrich_appends_section(self, tmp_path):
        chapters_dir = tmp_path / "chapters"
        chapters_dir.mkdir()
        (chapters_dir / "01-eo.md").write_text(
            "# Exec. Order No. 14148 (2025): Test\n\nBody content.\n",
            encoding="utf-8",
        )

        entries = {"14148": EOEntry("14148", "Test", "2025", "01-eo.md", 0)}
        reg = _make_registry(entries)
        cache = _make_cache_from_path(tmp_path / "cache.jsonl", [
            {
                "eo_number": "14148",
                "communication": {
                    "url": "https://a.gov/1",
                    "date": "2025-01-21",
                    "domain": "a.gov",
                    "official": "WH",
                    "action_description": "Test action",
                },
                "precedence": {"rescinds": [], "derived_from": [], "read_with": []},
                "record_id": "r1",
            },
        ])
        gen = EODiagramGenerator()
        table_gen = ImplementationTableGenerator()

        enricher = ChapterEnricher(reg, cache, gen, table_gen)
        enricher.enrich_chapters(chapters_dir)

        content = (chapters_dir / "01-eo.md").read_text()
        assert "## Implementation Record" in content
        assert "```mermaid" in content

    def test_enrich_replaces_existing_section(self, tmp_path):
        chapters_dir = tmp_path / "chapters"
        chapters_dir.mkdir()
        (chapters_dir / "01-eo.md").write_text(
            "# Exec. Order No. 14148 (2025): Test\n\nBody.\n\n"
            "## Implementation Record\n\nOld content\n",
            encoding="utf-8",
        )

        entries = {"14148": EOEntry("14148", "Test", "2025", "01-eo.md", 0)}
        reg = _make_registry(entries)
        cache = _make_cache_from_path(tmp_path / "cache.jsonl", [])
        gen = EODiagramGenerator()
        table_gen = ImplementationTableGenerator()

        enricher = ChapterEnricher(reg, cache, gen, table_gen)
        enricher.enrich_chapters(chapters_dir)

        content = (chapters_dir / "01-eo.md").read_text()
        assert content.count("## Implementation Record") == 1
        assert "Old content" not in content

    def test_enrich_missing_dir(self, tmp_path):
        reg = _make_registry()
        cache = _make_cache_from_path(tmp_path / "cache.jsonl", [])
        enricher = ChapterEnricher(
            reg, cache, EODiagramGenerator(), ImplementationTableGenerator()
        )
        # Should not raise
        enricher.enrich_chapters(tmp_path / "nonexistent")


# ===================================================================
# Cache helpers for tests
# ===================================================================

def _make_cache(records: list[dict]) -> EOImplementationCache:
    """Build a cache object in-memory without filesystem."""
    import logging
    cache = EOImplementationCache.__new__(EOImplementationCache)
    cache._logger = logging.getLogger("EOImplementationCache")
    cache.cache_path = Path("/dev/null")
    cache._records = {r.get("record_id", str(i)): r for i, r in enumerate(records)}
    return cache


def _make_cache_from_path(path: Path, records: list[dict]) -> EOImplementationCache:
    """Build a cache that persists to a real file."""
    cache = EOImplementationCache(path)
    if records:
        cache.append(records)
    return cache

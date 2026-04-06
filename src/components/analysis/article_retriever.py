"""
BM25 + Fuzzy Article Retriever for tagged BBOA media articles.

Loads tagged articles (unlinked + linked) into a per-field BM25 index with
optional fuzzy matching for person/country names. Supports structured queries
with AND/OR logic across: title, topic, media, operation, person, country.

Usage:
    retriever = ArticleRetriever.from_tagged_files()
    results = retriever.query(
        topic="national_security_foreign",
        title="Russia Ukraine",
        person=["Trump", "Hegseth"],
        country=["Russia", "Ukraine", "United States"],
        top_k=50,
    )

Each result is the full article record with an added ``_score`` field.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union

import numpy as np
from rank_bm25 import BM25Okapi
from rapidfuzz import fuzz

# ---------------------------------------------------------------------------
# Default data paths
# ---------------------------------------------------------------------------
_DEFAULT_UNLINKED = Path(
    "/mnt/workspace_2/bboa/workflows/media_linking/unlinked_articles_tagged.jsonl"
)
_DEFAULT_LINKED = Path(
    "/mnt/workspace_2/bboa/workflows/media_linking/media_linkage_cache_tagged.jsonl"
)

# ---------------------------------------------------------------------------
# Tokeniser
# ---------------------------------------------------------------------------
_SPLIT_RE = re.compile(r"\W+")


def _tokenize(text: str) -> list[str]:
    """Lowercase whitespace/punctuation split."""
    if not text:
        return []
    return [t for t in _SPLIT_RE.split(text.lower()) if t]


# ---------------------------------------------------------------------------
# Query spec
# ---------------------------------------------------------------------------
@dataclass
class ArticleQuery:
    """Structured query across tagged article fields.

    Each field can be:
      - None → skip this field
      - str  → single query string (space-separated = OR within field)
      - list[str] → multiple terms (OR logic by default)

    Set ``require_all=True`` for AND across fields (default: union/OR scoring).
    """
    title: Optional[Union[str, list[str]]] = None
    topic: Optional[Union[str, list[str]]] = None
    media: Optional[Union[str, list[str]]] = None
    operation: Optional[Union[str, list[str]]] = None
    person: Optional[Union[str, list[str]]] = None
    country: Optional[Union[str, list[str]]] = None
    source_filter: Optional[str] = None  # "linked", "unlinked", or None for both
    require_all: bool = False  # True = AND across fields, False = weighted sum
    top_k: int = 50


# ---------------------------------------------------------------------------
# Field weights (tunable)
# ---------------------------------------------------------------------------
_FIELD_WEIGHTS = {
    "title": 2.0,
    "topic": 3.0,     # exact category match is strong signal
    "media": 1.0,
    "operation": 2.5,
    "person": 2.0,
    "country": 1.5,
}

# Fuzzy match thresholds (rapidfuzz partial_ratio, 0-100)
_FUZZY_PERSON_THRESHOLD = 75
_FUZZY_COUNTRY_THRESHOLD = 80


# ---------------------------------------------------------------------------
# Article Retriever
# ---------------------------------------------------------------------------
class ArticleRetriever:
    """BM25 per-field index over tagged articles with fuzzy matching support."""

    def __init__(self, articles: list[dict]):
        """Build indices from a flat list of normalised article records.

        Each record must have: url, title, source, tags {topic, media, operation, person, country}
        Optionally: content (for summary text), linkage_rationale (for linked articles).
        """
        self._articles = articles
        self._n = len(articles)

        if self._n == 0:
            self._field_corpora = {}
            self._field_indices = {}
            self._person_lists = []
            self._country_lists = []
            return

        # Build per-field token corpora
        self._field_corpora: dict[str, list[list[str]]] = {}
        self._field_indices: dict[str, BM25Okapi] = {}

        # Pre-extract tag lists for fuzzy matching
        self._person_lists: list[list[str]] = []
        self._country_lists: list[list[str]] = []

        for art in articles:
            tags = art.get("tags", {})
            self._person_lists.append([p.lower() for p in tags.get("person", [])])
            self._country_lists.append([c.lower() for c in tags.get("country", [])])

        # Build BM25 indices for each searchable field
        self._build_field_index("title", [_tokenize(a.get("title", "")) for a in articles])
        self._build_field_index("topic", [_tokenize(" ".join(a.get("tags", {}).get("topic", []))) for a in articles])
        self._build_field_index("media", [_tokenize(" ".join(a.get("tags", {}).get("media", []))) for a in articles])
        self._build_field_index("operation", [_tokenize(" ".join(a.get("tags", {}).get("operation", []))) for a in articles])
        self._build_field_index("person", [_tokenize(" ".join(a.get("tags", {}).get("person", []))) for a in articles])
        self._build_field_index("country", [_tokenize(" ".join(a.get("tags", {}).get("country", []))) for a in articles])

    def _build_field_index(self, name: str, corpus: list[list[str]]) -> None:
        # BM25Okapi needs non-empty docs; pad empty with placeholder
        safe = [doc if doc else ["__empty__"] for doc in corpus]
        self._field_corpora[name] = safe
        self._field_indices[name] = BM25Okapi(safe)

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------
    @classmethod
    def from_tagged_files(
        cls,
        unlinked_path: Optional[Path] = None,
        linked_path: Optional[Path] = None,
    ) -> "ArticleRetriever":
        """Load and normalise tagged articles from JSONL files."""
        unlinked_path = unlinked_path or _DEFAULT_UNLINKED
        linked_path = linked_path or _DEFAULT_LINKED

        articles: list[dict] = []

        if unlinked_path.exists():
            with open(unlinked_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    articles.append({
                        "url": rec.get("url", ""),
                        "title": rec.get("title", ""),
                        "source": "unlinked",
                        "date": rec.get("date", ""),
                        "publisher": rec.get("publisher", ""),
                        "domain": rec.get("domain", ""),
                        "tags": rec.get("tags", {}),
                        "content": rec.get("content", ""),
                    })

        if linked_path.exists():
            with open(linked_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    articles.append({
                        "url": rec.get("article_url", ""),
                        "title": rec.get("article_title", ""),
                        "source": "linked",
                        "date": rec.get("article_date", ""),
                        "publisher": rec.get("publisher", ""),
                        "domain": rec.get("article_domain", ""),
                        "tags": rec.get("tags", {}),
                        "linkage_rationale": rec.get("linkage_rationale", ""),
                        "linked_eos": rec.get("linked_eos", []),
                        "confidence": rec.get("confidence", 0.0),
                    })

        return cls(articles)

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------
    def query(
        self,
        title: Optional[Union[str, list[str]]] = None,
        topic: Optional[Union[str, list[str]]] = None,
        media: Optional[Union[str, list[str]]] = None,
        operation: Optional[Union[str, list[str]]] = None,
        person: Optional[Union[str, list[str]]] = None,
        country: Optional[Union[str, list[str]]] = None,
        source_filter: Optional[str] = None,
        require_all: bool = False,
        top_k: int = 50,
    ) -> list[dict]:
        """Run a structured query and return top_k results sorted by combined score."""
        scores = np.zeros(self._n, dtype=np.float64)
        field_hits = np.zeros(self._n, dtype=np.int32)  # count of fields that matched

        field_queries = {
            "title": title,
            "topic": topic,
            "media": media,
            "operation": operation,
            "person": person,
            "country": country,
        }

        active_fields = 0
        for fname, qval in field_queries.items():
            if qval is None:
                continue
            active_fields += 1
            tokens = self._to_tokens(qval)
            if not tokens:
                continue

            weight = _FIELD_WEIGHTS.get(fname, 1.0)

            # BM25 scores
            bm25 = self._field_indices[fname]
            bm25_scores = bm25.get_scores(tokens)

            # Fuzzy boost for person/country
            if fname == "person":
                fuzzy_scores = self._fuzzy_boost_person(qval)
                bm25_scores = bm25_scores + fuzzy_scores
            elif fname == "country":
                fuzzy_scores = self._fuzzy_boost_country(qval)
                bm25_scores = bm25_scores + fuzzy_scores

            field_scores = bm25_scores * weight
            hit_mask = field_scores > 0
            scores += field_scores
            field_hits += hit_mask.astype(np.int32)

        # Require all: zero out docs that didn't match every active field
        if require_all and active_fields > 0:
            scores[field_hits < active_fields] = 0.0

        # Source filter
        if source_filter:
            for i, art in enumerate(self._articles):
                if art["source"] != source_filter:
                    scores[i] = 0.0

        # Rank
        top_indices = np.argsort(scores)[::-1][:top_k]
        results = []
        for idx in top_indices:
            if scores[idx] <= 0:
                break
            art = dict(self._articles[idx])
            art["_score"] = float(scores[idx])
            art["_field_hits"] = int(field_hits[idx])
            results.append(art)

        return results

    def query_from_spec(self, spec: ArticleQuery) -> list[dict]:
        """Run an ArticleQuery dataclass spec."""
        return self.query(
            title=spec.title,
            topic=spec.topic,
            media=spec.media,
            operation=spec.operation,
            person=spec.person,
            country=spec.country,
            source_filter=spec.source_filter,
            require_all=spec.require_all,
            top_k=spec.top_k,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _to_tokens(val: Union[str, list[str]]) -> list[str]:
        if isinstance(val, str):
            return _tokenize(val)
        # list of strings — tokenize each and combine
        tokens = []
        for v in val:
            tokens.extend(_tokenize(v))
        return tokens

    def _fuzzy_boost_person(self, query_val: Union[str, list[str]]) -> np.ndarray:
        """Fuzzy-match query person names against each article's person list."""
        boost = np.zeros(self._n, dtype=np.float64)
        terms = [query_val] if isinstance(query_val, str) else query_val

        for i, art_persons in enumerate(self._person_lists):
            if not art_persons:
                continue
            max_score = 0.0
            for qt in terms:
                qt_lower = qt.lower()
                for ap in art_persons:
                    ratio = fuzz.partial_ratio(qt_lower, ap)
                    if ratio >= _FUZZY_PERSON_THRESHOLD:
                        max_score = max(max_score, ratio / 100.0)
            boost[i] = max_score * 2.0  # scale to be competitive with BM25
        return boost

    def _fuzzy_boost_country(self, query_val: Union[str, list[str]]) -> np.ndarray:
        """Fuzzy-match query country names against each article's country list."""
        boost = np.zeros(self._n, dtype=np.float64)
        terms = [query_val] if isinstance(query_val, str) else query_val

        for i, art_countries in enumerate(self._country_lists):
            if not art_countries:
                continue
            max_score = 0.0
            for qt in terms:
                qt_lower = qt.lower()
                for ac in art_countries:
                    ratio = fuzz.partial_ratio(qt_lower, ac)
                    if ratio >= _FUZZY_COUNTRY_THRESHOLD:
                        max_score = max(max_score, ratio / 100.0)
            boost[i] = max_score * 1.5
        return boost

    # ------------------------------------------------------------------
    # Convenience: category-based retrieval
    # ------------------------------------------------------------------
    def query_category(
        self,
        category: str,
        extra_title: Optional[str] = None,
        extra_person: Optional[list[str]] = None,
        extra_country: Optional[list[str]] = None,
        extra_operation: Optional[list[str]] = None,
        source_filter: Optional[str] = None,
        top_k: int = 100,
    ) -> list[dict]:
        """Shorthand: retrieve articles matching a pipeline category with optional filters."""
        return self.query(
            title=extra_title,
            topic=category,
            person=extra_person,
            country=extra_country,
            operation=extra_operation,
            source_filter=source_filter,
            top_k=top_k,
        )

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------
    @property
    def article_count(self) -> int:
        return self._n

    def source_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for a in self._articles:
            s = a["source"]
            counts[s] = counts.get(s, 0) + 1
        return counts

    # ------------------------------------------------------------------
    # Format for LLM grounding
    # ------------------------------------------------------------------
    def format_for_grounding(self, results: list[dict], max_chars: int = 200_000) -> str:
        """Render BM25 results as a text block suitable for LLM prompts."""
        if not results:
            return ""
        lines = ["### BM25 Retrieved Media Articles", ""]
        total = 0
        for i, art in enumerate(results, 1):
            title = art.get("title", "Untitled")
            url = art.get("url", "")
            source = art.get("source", "")
            tags = art.get("tags", {})
            score = art.get("_score", 0)

            block = (
                f"#### {i}. [{source}] {title}\n"
                f"- **URL:** {url}\n"
                f"- **Score:** {score:.2f}\n"
                f"- **Topics:** {', '.join(tags.get('topic', []))}\n"
                f"- **Persons:** {', '.join(tags.get('person', []))}\n"
                f"- **Countries:** {', '.join(tags.get('country', []))}\n"
                f"- **Operations:** {', '.join(tags.get('operation', []))}\n"
            )
            # Add summary snippet for unlinked
            content = art.get("content", "")
            if content:
                idx = content.find("## Summary")
                snippet = content[idx + 10:idx + 500].strip() if idx >= 0 else content[:300]
                block += f"- **Summary:** {snippet}\n"
            rationale = art.get("linkage_rationale", "")
            if rationale:
                block += f"- **Linkage:** {rationale[:300]}\n"
            block += "\n"

            if total + len(block) > max_chars:
                break
            lines.append(block)
            total += len(block)
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# RAG Rerank: semantic retrieval with BM25 pre-filter
# ---------------------------------------------------------------------------
class RAGReranker:
    """Combine BM25 pre-filtering with RAG semantic retrieval.

    Flow:
      1. BM25 retrieves candidate articles by category/tags (fast, sparse)
      2. Candidate URLs sent to RAG /search for semantic scoring
      3. Results merged and reranked by combined score
      4. Relevance cutoff applied (default 0.35)
    """

    def __init__(
        self,
        retriever: ArticleRetriever,
        rag_url: str = "http://localhost:8000",
        relevance_cutoff: float = 0.35,
    ):
        self._retriever = retriever
        self._rag_url = rag_url.rstrip("/")
        self._cutoff = relevance_cutoff

    def query(
        self,
        search_query: str,
        topic: Optional[str] = None,
        person: Optional[Union[str, list[str]]] = None,
        country: Optional[Union[str, list[str]]] = None,
        operation: Optional[Union[str, list[str]]] = None,
        source_filter: Optional[str] = None,
        bm25_top_k: int = 200,
        rag_top_k: int = 100,
        include_qa: bool = True,
    ) -> list[dict]:
        """Two-stage retrieval: BM25 pre-filter → RAG semantic rerank.

        Args:
            search_query: Natural language query for RAG semantic search.
            topic: Category filter for BM25 pre-filtering.
            person/country/operation: BM25 tag filters.
            source_filter: "linked", "unlinked", or None.
            bm25_top_k: Number of BM25 candidates to retrieve.
            rag_top_k: Number of RAG results to request.
            include_qa: Whether to include QA pairs in RAG results.

        Returns:
            List of articles sorted by combined BM25+RAG score, above cutoff.
        """
        import requests

        # Stage 1: BM25 pre-filter
        bm25_results = self._retriever.query(
            title=search_query,
            topic=topic,
            person=person,
            country=country,
            operation=operation,
            source_filter=source_filter,
            top_k=bm25_top_k,
        )
        bm25_url_scores = {r["url"]: r["_score"] for r in bm25_results if r.get("url")}
        bm25_by_url = {r["url"]: r for r in bm25_results if r.get("url")}

        # Stage 2: RAG semantic search
        rag_results = []
        try:
            resp = requests.post(
                f"{self._rag_url}/search",
                json={
                    "query": search_query,
                    "top_k": rag_top_k,
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            rag_results = data.get("retrieved_docs", [])
        except Exception:
            pass  # RAG unavailable — fall back to BM25 only

        # Build RAG URL → score map
        rag_url_scores: dict[str, float] = {}
        for doc in rag_results:
            url = doc.get("source_url", "")
            score = doc.get("similarity_score", 0.0)
            if url:
                rag_url_scores[url] = score

        # Stage 3: merge scores (normalise BM25 to 0-1 range, combine)
        all_urls = set(bm25_url_scores) | set(rag_url_scores)
        max_bm25 = max(bm25_url_scores.values()) if bm25_url_scores else 1.0
        if max_bm25 == 0:
            max_bm25 = 1.0

        merged: list[dict] = []
        for url in all_urls:
            bm25_norm = bm25_url_scores.get(url, 0.0) / max_bm25
            rag_score = rag_url_scores.get(url, 0.0)
            combined = 0.4 * bm25_norm + 0.6 * rag_score  # weight RAG higher

            if combined < self._cutoff:
                continue

            art = bm25_by_url.get(url, {"url": url, "source": "rag"})
            art = dict(art)
            art["_bm25_score"] = bm25_url_scores.get(url, 0.0)
            art["_rag_score"] = rag_score
            art["_combined_score"] = combined
            merged.append(art)

        merged.sort(key=lambda x: x["_combined_score"], reverse=True)
        return merged

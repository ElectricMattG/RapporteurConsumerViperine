
import sys
from pathlib import Path

# 1. Delete bad lines
filepath = "src/components/analysis/eo_linking.py"
start_line = 2356

try:
    with open(filepath, 'r') as f:
        lines = f.readlines()

    if len(lines) >= start_line:
        print(f"Trimming {filepath} from line {start_line}...")
        with open(filepath, 'w') as f:
            f.writelines(lines[:start_line-1])
    else:
        print(f"File {filepath} has fewer than {start_line} lines. Skipping trim.")

except Exception as e:
    print(f"Error reading file: {e}")
    sys.exit(1)

# 2. Append Corrected Code
content = r'''
# ---------------------------------------------------------------------------
# Phase 7 — Chapter Integration
# ---------------------------------------------------------------------------

class ChapterEnricher(Base):
    """Inject implementation sections into EO book chapters."""

    SECTION_HEADER = "## Implementation Record"

    def __init__(
        self,
        registry: EORegistry,
        cache: EOImplementationCache,
        diagram_gen: EODiagramGenerator,
        table_gen: ImplementationTableGenerator,
    ) -> None:
        super().__init__()
        self.registry = registry
        self.cache = cache
        self.diagram_gen = diagram_gen
        self.table_gen = table_gen

    def enrich_chapters(self, chapters_dir: str | Path) -> None:
        """Process all markdown chapters and inject implementation records.

        Iterates through the *already loaded* chapters in the registry to find
        valid target files, then updates them in place.
        """
        base_dir = Path(chapters_dir)
        if not base_dir.is_dir():
            self._logger.error("Chapters directory not found: %s", base_dir)
            return

        # Pre-calculate precedence graph (dict form) for diagrams
        # The cache returns dict already: {eo: {rescinds: [], ...}}
        precedence_graph = self.cache.precedence_graph()

        updated_count = 0

        # Iterate over known EO entries that have a chapter file
        for eo_number, entry in self.registry.entries.items():
            if not entry.chapter_file:
                continue

            chapter_path = base_dir / entry.chapter_file
            if not chapter_path.exists():
                self._logger.warning("Chapter file missing: %s", chapter_path)
                continue

            self._logger.info("Enriching chapter for EO %s: %s",
                            eo_number, entry.chapter_file)

            # 1. Generate Diagram (Mermaid)
            # Use static helper from diagram generator
            merged_graph = self.diagram_gen._merge_supercessions(precedence_graph, self.registry)

            diagram_body = self.diagram_gen.generate(
                eo_number, merged_graph, self.registry
            )

            # Wrap in mermaid block
            diagram_md = f"```mermaid\n{diagram_body}\n```"

            # 2. Generate Table
            table_md = self.table_gen.generate(eo_number, self.cache)
            if not table_md:
                 table_md = "_No official communications linked to this EO yet._"

            # 3. Construct Section
            implementation_section = (
                f"\n\n{self.SECTION_HEADER}\n\n"
                f"### EO Precedence\n\n"
                f"{diagram_md}\n\n"
                f"### Official Communications Timeline\n\n"
                f"{table_md}\n"
            )

            # 4. Update File
            try:
                original_content = chapter_path.read_text(encoding="utf-8")

                if self.SECTION_HEADER in original_content:
                    # Replace existing section - assume it is at the end
                    parts = original_content.split(self.SECTION_HEADER)
                    new_content = parts[0].rstrip() + implementation_section
                else:
                    # Append to end
                    new_content = original_content.rstrip() + implementation_section

                chapter_path.write_text(new_content, encoding="utf-8")
                updated_count += 1
            except Exception as e:
                self._logger.error("Failed to update chapter %s: %s", chapter_path.name, e)

        self._logger.info("Enriched %d chapters with implementation records", updated_count)
'''

with open(filepath, "a") as f:
    f.write(content)
print("Successfully fixed src/components/analysis/eo_linking.py")

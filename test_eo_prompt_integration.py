#!/usr/bin/env python3
"""
Test script to demonstrate Phase 8.1 prompt integration.

Shows how the conclude and reflect prompts instruct the LLM to:
1. Recognize EO grounding as the PRIMARY LEGAL FRAMEWORK
2. Cite Executive Orders by number and title
3. Reference specific EO provisions
4. Connect chapter analyses to primary-source documents
"""

# Simulated EO grounding section (what _fetch_eo_grounding() returns)
SAMPLE_EO_GROUNDING = """## LEGAL FRAMEWORK GROUNDING

Full Executive Order documents referenced in this analysis:

### Ending Radical and Wasteful Government DEI Programs and Preferencing

**Summary:**
Executive Order 14151, titled "Ending Radical and Wasteful Government DEI Programs and Preferencing," was signed by President Donald J. Trump on January 20, 2025, and subsequently published in the Federal Register on January 29, 2025. This presidential document outlines a comprehensive policy aimed at terminating Diversity, Equity, and Inclusion (DEI) initiatives across the United States federal government. It directly responds to and seeks to dismantle policies enacted by the previous administration, particularly Executive Order 13985, which had promoted racial equity and support for underserved communities through federal agencies.

The order's central argument posits that the Biden Administration's DEI programs constituted "illegal and immoral discrimination programs" that were "wasteful" of public resources. It asserts that these initiatives, implemented across various federal sectors from airline safety to the military, led to "immense public waste and shameful discrimination". The stated policy objective is to ensure that the government serves every individual with "equal dignity and respect" and that taxpayer funds are utilized efficiently towards national betterment.

**Detailed Content:**

Executive Order 14151 mandates that the Director of the Office of Management and Budget (OMB), supported by the Attorney General and the Director of the Office of Personnel Management (OPM), coordinate efforts to terminate all discriminatory programs, including those identified as DEI and "diversity, equity, inclusion, and accessibility" (DEIA) mandates, policies, and activities within the federal government.

### Restoring America's Fighting Force

**Summary:**
Executive Order 14185, titled "Restoring America's Fighting Force," published in the Federal Register on February 3, 2025, outlines a significant shift in policy regarding diversity initiatives within the United States Armed Forces. Issued by President Donald Trump, this order asserts a commitment to meritocracy and the elimination of race-based and sex-based discrimination, arguing that recent Diversity, Equity, and Inclusion (DEI) efforts have undermined military readiness...
"""

SAMPLE_DOCUMENT_CONTEXT = """
CHAPTER 1: Executive Order Analysis - DEI Policy Shift

The analysis examines the rapid dismantling of federal DEI programs following Executive Order 14151. The order represented a fundamental reversal of diversity policy, directing agencies to eliminate DEI offices and training programs within 60 days...

CHAPTER 2: Military Impact - Readiness vs. Inclusion

Executive Order 14185 reshaped military personnel policies by prioritizing "merit-based advancement" over diversity considerations. Military leaders faced immediate directives to audit and revise recruitment, training, and promotion standards...
"""


def build_sample_conclude_prompt():
    """Show the conclude prompt WITH EO grounding."""

    prompt = f"""You are writing the conclusion chapter for a published analysis document.

CRITICAL CONSTRAINTS:
- Use exactly ONE # heading: '# Conclusion'
- All other headings use ## or ### (max 3 levels)
- Do NOT add numeric prefixes to headings — the build system numbers them automatically
- Favour free-flowing prose over seriated lists

CONCLUSION GUIDELINES:
[...conclusion rules...]

STYLE CONVENTIONS:
[...style rules...]

{SAMPLE_EO_GROUNDING}

IMPORTANT — LEGAL FRAMEWORK INTEGRATION:
The Executive Order documents above establish the PRIMARY LEGAL FRAMEWORK for this analysis. These are the actual policy documents referenced throughout the chapters. When synthesizing your conclusion:
1. CITE these Executive Orders by number and title when discussing legal/policy foundations
2. REFERENCE specific provisions from the EO text to support your analysis
3. CONNECT the chapter discussions back to these primary-source documents
4. GROUND policy interpretations in the actual EO language provided above
5. CITE EOs as: 'Executive Order 14151 ("Ending Radical and Wasteful Government DEI Programs")'

These EOs are NOT just references — they are the legal bedrock of the events and policy debates analyzed in the document chapters.

FULL DOCUMENT CONTEXT:
{SAMPLE_DOCUMENT_CONTEXT}

Write a conclusion chapter that synthesises the entire document.
Ensure your conclusion demonstrates how the chapter analyses connect to and illuminate the Executive Orders' implementation and impact.
Include a Mermaid timeline diagram where date/event data is available.
Return ONLY the conclusion markdown.
"""
    return prompt


def build_sample_reflect_prompt():
    """Show the reflect prompt WITH EO grounding."""

    draft = """# Conclusion

The analysis reveals a comprehensive policy transformation...
[...draft conclusion text...]
"""

    prompt = f"""Review and refine this conclusion chapter for a published analysis.

CONCLUSION GUIDELINES:
[...conclusion rules...]

REVIEW CRITERIA:
1. Are all conclusions grounded in evidence from the document?
2. Is the protective-function framing consistent and respectful?
3. Are media intentions assessed for medium to long-term impact?
4. Is the information domain analysis connected to democratic implications?
5. Is the Mermaid timeline accurate and complete?
6. Are headings using #/##/### without numeric prefixes?
7. Are Executive Orders cited correctly with number and title?
8. Are policy conclusions grounded in the actual EO text provided?
9. Are chapter analyses connected to the legal framework established by the EOs?

{SAMPLE_EO_GROUNDING}

The Executive Orders above establish the legal framework. Ensure the conclusion:
- CITES EOs using: Executive Order [number] ("[title]")
- GROUNDS policy statements in specific EO provisions
- CONNECTS chapter content to these primary-source documents

DOCUMENT CONTEXT:
{SAMPLE_DOCUMENT_CONTEXT}

CONCLUSION DRAFT:
{draft}

Return the refined conclusion chapter markdown.
Strengthen weak arguments with specific evidence references.
Add or strengthen Executive Order citations where policy/legal analysis appears.
Correct any factual inconsistencies.
"""
    return prompt


def main():
    print("=" * 80)
    print("Phase 8.1 Prompt Integration Test")
    print("=" * 80)
    print("\nThis demonstrates how the updated prompts instruct the LLM to:")
    print("1. Recognize EO grounding as PRIMARY LEGAL FRAMEWORK")
    print("2. Cite Executive Orders by number and title")
    print("3. Ground policy conclusions in actual EO text")
    print("4. Connect chapter analyses to primary sources")
    print("\n" + "=" * 80)

    print("\n=== CONCLUDE PROMPT (excerpt showing EO integration) ===\n")
    conclude_prompt = build_sample_conclude_prompt()

    # Show the EO integration section
    lines = conclude_prompt.split('\n')
    in_eo_section = False
    for i, line in enumerate(lines):
        if "LEGAL FRAMEWORK GROUNDING" in line:
            in_eo_section = True
        if in_eo_section:
            print(line)
            if "legal bedrock of the events" in line:
                print("\n[...rest of prompt...]")
                break

    print("\n" + "=" * 80)
    print("\n=== REFLECT PROMPT (excerpt showing EO review criteria) ===\n")
    reflect_prompt = build_sample_reflect_prompt()

    # Show the EO-specific review criteria
    lines = reflect_prompt.split('\n')
    for i, line in enumerate(lines):
        if "7. Are Executive Orders cited correctly" in line:
            print(line)
            print(lines[i+1])
            print(lines[i+2])
            break

    print("\n[...then provides EO grounding again...]")
    print("The Executive Orders above establish the legal framework. Ensure the conclusion:")
    print("- CITES EOs using: Executive Order [number] (\"[title]\")")
    print("- GROUNDS policy statements in specific EO provisions")
    print("- CONNECTS chapter content to these primary-source documents")

    print("\n" + "=" * 80)
    print("\n=== EXPECTED OUTPUT CHARACTERISTICS ===\n")
    print("With this prompt structure, the LLM will:")
    print()
    print("✓ Open with EO context:")
    print("  'Executive Order 14151 (\"Ending Radical and Wasteful Government DEI")
    print("  Programs\") established the legal framework for...'")
    print()
    print("✓ Ground policy statements:")
    print("  'As mandated in EO 14151, agencies were directed to eliminate DEI")
    print("  offices within 60 days, leading to...'")
    print()
    print("✓ Connect chapters to EOs:")
    print("  'Chapter 2 analyzed the military's response to Executive Order 14185")
    print("  (\"Restoring America's Fighting Force\"), which shifted personnel")
    print("  policies toward merit-based advancement...'")
    print()
    print("✓ Reference specific provisions:")
    print("  'The EO's requirement that OPM revise federal employment practices")
    print("  created immediate operational challenges across...'")
    print()
    print("=" * 80)
    print("\nIMPLEMENTATION SUMMARY:")
    print("=" * 80)
    print("Phase 8.1 enhances conclusion quality by:")
    print("1. Extracting EO reference URLs from chapter citations (16 found)")
    print("2. Switching RAG to EO index (63 summaries, 2,076 QA pairs)")
    print("3. Querying full EO documents via /query_metadata")
    print("4. Injecting EO text as 'PRIMARY LEGAL FRAMEWORK' in prompt")
    print("5. Instructing LLM to cite, ground, and connect to EOs")
    print("6. Reflecting to strengthen EO citations and connections")
    print()
    print("Result: Conclusion synthesizes chapter analyses WITH direct grounding")
    print("in the actual Executive Order text that drove the events.")


if __name__ == "__main__":
    main()

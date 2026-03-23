import json


def _format_hero_schema(hero_with_desc):
    """Format hero fields with descriptions and expected types for the prompt."""
    lines = []
    for i, (name, meta) in enumerate(hero_with_desc.items(), 1):
        desc = meta.get("description", "")
        mandatory = meta.get("mandatory_column", "Optional")
        expected = meta.get("expected_type", "text")
        lines.append(f'  {i}. "{name}" [{mandatory}] (expects: {expected}): {desc}')
    return "\n".join(lines)


def _format_registry(registry):
    """Format source columns with samples, types, and semantic info for the prompt."""
    lines = []
    for entry in registry:
        samples = entry.get("samples", [])[:8]
        sample_str = ", ".join(f'"{s}"' for s in samples)
        dtype = entry.get("dtype", "unknown")
        null_pct = round(entry.get("null_ratio", 0) * 100, 1)
        semantic = entry.get("semantic_type", "unknown")
        distinct = entry.get("distinct_count", 0)
        flags = []
        if entry.get("is_numeric"):
            flags.append("numeric")
        if entry.get("is_date"):
            flags.append("date")
        flag_str = f" ({', '.join(flags)})" if flags else ""

        lines.append(
            f'  - File: "{entry["file"]}" | Column: "{entry["column"]}" '
            f"| Type: {dtype}{flag_str} | Semantic: {semantic} "
            f"| Nulls: {null_pct}% | Distinct: {distinct} "
            f"| Samples: [{sample_str}]"
        )
    return "\n".join(lines)


def build_global_prompt(registry, hero_cols, hero_full=None):
    """
    Build the mapping prompt with strict one-to-one, type-safety,
    and semantic-awareness constraints.
    """

    if hero_full:
        hero_section = _format_hero_schema(hero_full)
    else:
        hero_section = "\n".join(f'  {i}. "{c}"' for i, c in enumerate(hero_cols, 1))

    source_section = _format_registry(registry)
    num_hero = len(hero_full) if hero_full else len(hero_cols)
    num_source = len(registry)

    prompt = (
        "You are an expert data onboarding analyst for a self-storage management platform.\n\n"
        f"TASK: Map source columns from uploaded customer files to the Hero Format schema.\n"
        f"There are {num_hero} hero fields and {num_source} source columns across multiple files.\n\n"
        "HERO FORMAT SCHEMA (target fields with descriptions and expected data types):\n"
        f"{hero_section}\n\n"
        "SOURCE COLUMNS (from uploaded files with sample data and detected types):\n"
        f"{source_section}\n\n"

        "═══════════════════════════════════════════════════\n"
        "STRICT RULES — VIOLATIONS WILL CAUSE IMPORT FAILURE\n"
        "═══════════════════════════════════════════════════\n\n"

        "RULE 1 — ONE-TO-ONE MAPPING (MOST IMPORTANT):\n"
        "  Each source column (file + column combination) can be mapped to AT MOST ONE hero field.\n"
        "  NEVER map the same source column to multiple hero fields.\n"
        '  Example violation: mapping source "move_in_date" to BOTH "Move In Date" AND "start_date" is FORBIDDEN.\n'
        '  If a source column could fit multiple hero fields, pick the SINGLE BEST match and leave others unmapped.\n\n'

        "RULE 2 — DATA TYPE COMPATIBILITY (CRITICAL):\n"
        "  The sample values in the source column MUST match the expected data type of the hero field.\n"
        "  Type rules:\n"
        '  - person_name fields (First Name, Last Name, etc.): values MUST be human names like "John", "Sarah".\n'
        '    NEVER map phone numbers, emails, IDs, dates, or numeric values to person_name fields.\n'
        '  - email fields: values MUST contain "@" and look like email addresses.\n'
        '  - phone fields: values MUST be phone number patterns (digits, dashes, parentheses).\n'
        '  - numeric fields (Rent, Rate, Width, Length, balances, etc.): values MUST be numbers or currency amounts.\n'
        '    NEVER map text strings, names, dates, or codes into numeric fields.\n'
        '  - date fields: values MUST be parseable dates (2024-01-15, 01/15/2024, Jan 15 2024, etc.).\n'
        '  - boolean fields (IsBusinessLease, Alarm Enabled, etc.): values MUST be yes/no, true/false, 1/0, Y/N.\n'
        '    NEVER map names, text descriptions, or other data into boolean fields.\n'
        "  - identifier fields (Space, Account Code, ZIP): alphanumeric codes or IDs.\n"
        "  If the sample values don't match the expected type, DO NOT map that column.\n\n"

        "RULE 3 — SEMANTIC MATCHING (USE SAMPLE DATA AS GROUND TRUTH):\n"
        "  ALWAYS verify mappings by examining the actual sample values, not just column names.\n"
        "  The sample data is the final arbiter of correctness.\n"
        '  - A column named "Name" with samples ["john@email.com", "sara@test.com"] is email data, NOT a name field.\n'
        '  - A column named "Code" with samples ["John", "Mary"] is person name data, NOT an identifier.\n'
        '  - A column named "Details" with samples ["555-1234", "555-5678"] is phone data.\n\n'

        "RULE 4 — DISTINGUISHING SIMILAR HERO FIELDS:\n"
        '  These field pairs are semantically DIFFERENT — do not confuse them:\n'
        '  - "Move In Date" = date tenant physically moved in. "start_date" = lease agreement start date.\n'
        '    Map a source column to only ONE of these based on its description/context.\n'
        '  - "First Name" = tenant\'s first name. "IsBusinessLease" = boolean flag (yes/no).\n'
        '    Customer names MUST go to First Name/Last Name, NEVER to IsBusinessLease.\n'
        '  - "Rate" = base/standard rent. "Rent" = current charged rent. "Web Rate" = online price.\n'
        '  - "Address" = tenant billing address. "Alt Address" = alternate contact address.\n'
        '  - "Cell Phone" vs "Home Phone" vs "Work Phone" — match based on column name context.\n\n'

        "RULE 5 — MATCHING CONFIDENCE:\n"
        "  Match by MEANING and DATA CONTENT, not just column name similarity.\n"
        "  Use the description and expected type of each hero field to understand what data it expects.\n"
        "  If multiple source columns could match the same hero field, pick the best based on:\n"
        "  a) Data type compatibility (strict requirement)\n"
        "  b) Sample value quality (lower null ratio, matching content)\n"
        "  c) Column name relevance\n"
        "  If no source column matches, do NOT include that hero field.\n\n"

        "CONFIDENCE SCORING:\n"
        "  0.9-1.0: exact/near-exact name match AND data type+content match perfectly\n"
        "  0.7-0.89: name differs but data content clearly matches the description and type\n"
        "  0.5-0.69: partial match, data somewhat fits but ambiguous\n"
        "  below 0.5: weak match — prefer leaving unmapped over a bad mapping\n\n"

        "OUTPUT FORMAT — respond with ONLY valid JSON, no markdown, no extra text:\n\n"
        "  For Excel files with multiple sheets, the source 'file' is \"filename.xlsx::SheetName\".\n"
        "  Use the EXACT file (and sheet) string from the SOURCE COLUMNS list for each mapping.\n\n"
        "{\n"
        '  "Hero Field Name": {\n'
        '    "file": "source_filename.xlsx or source_filename.xlsx::SheetName",\n'
        '    "column": "source_column_name",\n'
        '    "confidence": 0.85,\n'
        '    "reason": "brief explanation including what sample values confirm the match"\n'
        "  }\n"
        "}\n\n"
        "FINAL CHECKLIST BEFORE RESPONDING:\n"
        "[ ] No source column appears more than once across all mappings\n"
        "[ ] Every mapped source column's sample values match the hero field's expected type\n"
        "[ ] Person names are ONLY in person_name fields, phones ONLY in phone fields, etc.\n"
        "[ ] Boolean hero fields (IsBusinessLease, Alarm Enabled, etc.) only have yes/no/true/false data\n"
        "[ ] Hero field names are EXACTLY as listed (case-sensitive)\n"
        '[ ] "file" and "column" exactly match the source names provided\n'
        "[ ] Output is valid JSON (no trailing commas, no comments, no markdown)\n"
        "[ ] Response starts with { and ends with }"
    )

    return prompt


def safe_parse(text):
    text = text.strip()
    try:
        return json.loads(text)
    except Exception:
        pass

    import re
    match = re.search(r"\{.*\}", text, re.S)
    if match:
        try:
            return json.loads(match.group())
        except Exception:
            pass

    raise ValueError("Could not parse LLM JSON:\n" + text)

import re
import logging
from collections import defaultdict

from backend.hero_schema import load_hero_schema_full, FIELD_EXPECTED_TYPES

logger = logging.getLogger("post_validate")

_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")
_PHONE_RE = re.compile(r"^[\d\s\(\)\-\+\.]{7,20}$")
_BOOLEAN_VALUES = {"yes", "no", "true", "false", "y", "n", "1", "0", "1.0", "0.0"}


def _samples_match_type(samples, expected_type):
    """
    Check whether the majority of sample values are compatible
    with the expected data type of the hero field.
    Returns (is_compatible: bool, reason: str).
    """
    if not samples:
        return True, "no samples to check"

    clean = [str(s).strip() for s in samples if str(s).strip()]
    if not clean:
        return True, "empty samples"

    total = len(clean)

    if expected_type == "numeric":
        numeric_ok = 0
        for v in clean:
            v_clean = v.replace(",", "").replace("$", "").replace("%", "").strip()
            try:
                float(v_clean)
                numeric_ok += 1
            except (ValueError, TypeError):
                pass
        ratio = numeric_ok / total
        if ratio < 0.5:
            return False, f"only {numeric_ok}/{total} samples are numeric: {clean[:3]}"
        return True, "numeric OK"

    if expected_type == "email":
        email_ok = sum(1 for v in clean if _EMAIL_RE.match(v))
        if email_ok / total < 0.3:
            return False, f"only {email_ok}/{total} look like emails: {clean[:3]}"
        return True, "email OK"

    if expected_type == "phone":
        phone_ok = sum(
            1 for v in clean
            if _PHONE_RE.match(v) and sum(c.isdigit() for c in v) >= 7
        )
        if phone_ok / total < 0.3:
            return False, f"only {phone_ok}/{total} look like phones: {clean[:3]}"
        return True, "phone OK"

    if expected_type == "person_name":
        name_ok = 0
        for v in clean:
            alpha_ratio = sum(c.isalpha() or c == ' ' for c in v) / max(len(v), 1)
            has_at = '@' in v
            digit_ratio = sum(c.isdigit() for c in v) / max(len(v), 1)
            if alpha_ratio > 0.7 and not has_at and digit_ratio < 0.3:
                name_ok += 1
        if name_ok / total < 0.5:
            return False, f"only {name_ok}/{total} look like person names: {clean[:3]}"
        return True, "person_name OK"

    if expected_type == "boolean":
        bool_ok = sum(1 for v in clean if v.lower().strip() in _BOOLEAN_VALUES)
        if bool_ok / total < 0.5:
            return False, f"only {bool_ok}/{total} are boolean values: {clean[:3]}"
        return True, "boolean OK"

    if expected_type == "date":
        import pandas as pd
        date_ok = 0
        for v in clean:
            try:
                pd.to_datetime(v)
                date_ok += 1
            except Exception:
                pass
        if date_ok / total < 0.3:
            return False, f"only {date_ok}/{total} parse as dates: {clean[:3]}"
        return True, "date OK"

    return True, "no strict type check"


def validate_mapping(mapping, registry):
    """
    Post-mapping validation that catches and auto-fixes issues:
    1. Duplicate source columns mapped to multiple hero fields
    2. Type mismatches (e.g., phone data in a person_name field)
    3. Cross-field semantic conflicts

    Returns (cleaned_mapping, issues_log).
    """
    hero_full = load_hero_schema_full()
    issues = []

    registry_lookup = {}
    for entry in registry:
        key = (entry["file"], entry["column"])
        registry_lookup[key] = entry

    # --- CHECK 1: Detect duplicate source column mappings ---
    source_to_hero = defaultdict(list)
    for hero_field, info in mapping.items():
        if not isinstance(info, dict):
            continue
        src_file = info.get("file")
        src_col = info.get("column")
        if src_file and src_col:
            source_key = (src_file, src_col)
            source_to_hero[source_key].append({
                "hero_field": hero_field,
                "confidence": info.get("confidence", 0),
            })

    duplicates_removed = set()
    for source_key, hero_entries in source_to_hero.items():
        if len(hero_entries) <= 1:
            continue

        hero_entries.sort(key=lambda x: -x["confidence"])
        winner = hero_entries[0]["hero_field"]
        for loser in hero_entries[1:]:
            loser_field = loser["hero_field"]
            issues.append({
                "type": "duplicate_source",
                "severity": "error",
                "source": f"{source_key[0]}:{source_key[1]}",
                "kept": winner,
                "removed": loser_field,
                "message": (
                    f"Source column '{source_key[1]}' (file: {source_key[0]}) "
                    f"was mapped to both '{winner}' and '{loser_field}'. "
                    f"Keeping '{winner}' (confidence {hero_entries[0]['confidence']:.2f}), "
                    f"removing '{loser_field}' (confidence {loser['confidence']:.2f})."
                ),
            })
            duplicates_removed.add(loser_field)

    # --- CHECK 2: Type mismatch validation ---
    type_removed = set()
    for hero_field, info in mapping.items():
        if hero_field in duplicates_removed:
            continue
        if not isinstance(info, dict):
            continue

        expected_type = FIELD_EXPECTED_TYPES.get(hero_field, "text")
        if expected_type == "text":
            continue

        src_file = info.get("file")
        src_col = info.get("column")
        source_key = (src_file, src_col)

        reg = registry_lookup.get(source_key, {})
        samples = reg.get("samples", [])
        semantic = reg.get("semantic_type", "unknown")

        is_ok, reason = _samples_match_type(samples, expected_type)
        if not is_ok:
            issues.append({
                "type": "type_mismatch",
                "severity": "error",
                "hero_field": hero_field,
                "expected_type": expected_type,
                "source_semantic": semantic,
                "source": f"{src_file}:{src_col}",
                "message": (
                    f"Hero field '{hero_field}' expects {expected_type} data, "
                    f"but source '{src_col}' contains {semantic} data. {reason}"
                ),
            })
            type_removed.add(hero_field)

        # Extra: specific cross-type checks
        if expected_type == "person_name" and semantic in ("email", "phone", "numeric"):
            if hero_field not in type_removed:
                issues.append({
                    "type": "semantic_conflict",
                    "severity": "error",
                    "hero_field": hero_field,
                    "message": (
                        f"'{hero_field}' (person_name) mapped to column with {semantic} data. Removing."
                    ),
                })
                type_removed.add(hero_field)

        if expected_type == "boolean" and semantic not in ("boolean", "unknown", "empty"):
            if hero_field not in type_removed:
                issues.append({
                    "type": "semantic_conflict",
                    "severity": "error",
                    "hero_field": hero_field,
                    "message": (
                        f"'{hero_field}' (boolean) mapped to column with {semantic} data. Removing."
                    ),
                })
                type_removed.add(hero_field)

    # --- BUILD CLEANED MAPPING ---
    cleaned = {}
    all_removed = duplicates_removed | type_removed
    for hero_field, info in mapping.items():
        if hero_field in all_removed:
            continue
        cleaned[hero_field] = info

    removed_count = len(all_removed)
    if removed_count > 0:
        logger.warning(
            f"Post-validation removed {removed_count} mappings: "
            f"{len(duplicates_removed)} duplicates, {len(type_removed)} type mismatches"
        )
    else:
        logger.info("Post-validation passed: no issues found")

    return cleaned, issues

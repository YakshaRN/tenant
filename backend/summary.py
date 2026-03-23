from backend.hero_schema import load_hero_schema, load_hero_schema_full


def build_summary(pre, mapping, tags, timings):
    hero = load_hero_schema()
    hero_full = load_hero_schema_full()

    coverage = len(mapping) / max(len(hero), 1)

    low_conf = [
        k for k, v in mapping.items()
        if v.get("confidence", 1) < 0.6
    ]

    mandatory_fields = [k for k, v in hero_full.items() if v.get("mandatory_column") == "Mandatory"]
    mandatory_mapped = [f for f in mandatory_fields if f in mapping]
    mandatory_missing = [f for f in mandatory_fields if f not in mapping]

    anomaly_summary = {}
    if isinstance(tags, list) and tags and isinstance(tags[0], dict):
        for t in tags:
            cat = t.get("category", "unknown")
            anomaly_summary[cat] = anomaly_summary.get(cat, 0) + 1

    has_anomalies = len(tags) > 0
    has_low_conf = len(low_conf) > 0

    if has_anomalies or has_low_conf:
        status = "warning"
    else:
        status = "success"

    return {
        "status": status,
        "coverage": round(coverage, 4),
        "total_hero_fields": len(hero),
        "mapped_fields": len(mapping),
        "mandatory_mapped": len(mandatory_mapped),
        "mandatory_total": len(mandatory_fields),
        "mandatory_missing": mandatory_missing,
        "low_confidence_fields": low_conf,
        "files_used": list(set(v.get("file", "") for v in mapping.values() if v.get("file"))),
        "timings": timings,
        "anomaly_count": len(tags),
        "anomaly_summary": anomaly_summary,
        "anomalies": tags,
    }

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

import boto3
import os
import uuid
import json
import datetime
import time
import io
import logging
import pandas as pd

from backend.prevalidate import prevalidate
from backend.bedrock import call_llm
from backend.mapper import build_global_prompt
from backend.tagger import detect
from backend.summary import build_summary
from backend.hero_schema import load_hero_schema, load_hero_schema_full
from backend.column_registry import extract_columns
from backend.json_utils import extract_json
from backend.post_validate import validate_mapping
from backend.database import init_db, SessionLocal
from backend.sql_ingestion import ingest_hero_to_sql
from backend.models import (
    Facility, Space, Tenant, AlternateContact, Lease,
    FinancialBalance, InsuranceCoverage, Lien,
    Promotion, Discount, MilitaryDetail,
)

logger = logging.getLogger("main")

app = FastAPI()

@app.on_event("startup")
def on_startup():
    init_db()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

s3 = boto3.client("s3")

BUCKET = os.getenv("S3_BUCKET", "tenant-onboarding-186189159698-20260324")
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB per file
MAX_FILES = 15


def create_job():
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"job_{ts}_{uuid.uuid4().hex[:6]}"


def list_raw_files(job_id):
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"jobs/{job_id}/raw/")
    if "Contents" not in resp:
        raise HTTPException(404, "No files found")
    files = [obj for obj in resp["Contents"] if not obj["Key"].endswith("/")]
    if not files:
        raise HTTPException(404, "No valid files found")
    return files


def load_file(key):
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def read_dataframe(data, name):
    """Read a single file into one DataFrame (first sheet for Excel). Kept for backward compatibility."""
    if name.lower().endswith(".csv"):
        return pd.read_csv(io.BytesIO(data))
    else:
        return pd.read_excel(io.BytesIO(data))


def read_all_sheets(data, name):
    """
    Read a file into one or more (source_key, df) pairs.
    CSV: [(filename, df)]. Excel: [(filename::Sheet1, df1), (filename::Sheet2, df2), ...].
    """
    out = []
    if name.lower().endswith(".csv"):
        df = pd.read_csv(io.BytesIO(data))
        out.append((name, df))
        return out
    xl = pd.ExcelFile(io.BytesIO(data))
    for sheet_name in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=sheet_name)
        source_key = f"{name}::{sheet_name}"
        out.append((source_key, df))
    return out


@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/upload")
async def upload(files: list[UploadFile] = File(...)):
    if not files:
        raise HTTPException(400, "No files uploaded")

    if len(files) > MAX_FILES:
        raise HTTPException(400, f"Too many files. Maximum is {MAX_FILES}")

    job_id = create_job()
    uploaded = []
    total_size = 0

    for f in files:
        content = await f.read()
        file_size = len(content)

        if file_size > MAX_FILE_SIZE:
            raise HTTPException(
                400,
                f"File {f.filename} is {file_size // (1024*1024)}MB, exceeds {MAX_FILE_SIZE // (1024*1024)}MB limit"
            )

        total_size += file_size
        key = f"jobs/{job_id}/raw/{f.filename}"
        s3.put_object(Bucket=BUCKET, Key=key, Body=content)
        uploaded.append(key)

    logger.info(f"Job {job_id}: uploaded {len(uploaded)} files, total {total_size // 1024}KB")

    meta = {
        "job_id": job_id,
        "status": "uploaded",
        "files": uploaded,
        "file_count": len(uploaded),
        "total_size_kb": total_size // 1024,
        "created_at": datetime.datetime.utcnow().isoformat()
    }
    s3.put_object(Bucket=BUCKET, Key=f"jobs/{job_id}/meta.json", Body=json.dumps(meta))
    return meta


def _run_prevalidation(job_id):
    objs = list_raw_files(job_id)
    hero_full = load_hero_schema_full()
    results = []
    for obj in objs:
        key = obj["Key"]
        data = load_file(key)
        name = key.split("/")[-1]
        if name.lower().endswith(".csv"):
            r = prevalidate(data, name)
            results.append({"file": name, "result": r})
        else:
            xl = pd.ExcelFile(io.BytesIO(data))
            for sheet_name in xl.sheet_names:
                r = prevalidate(data, name, sheet_name=sheet_name)
                source_key = f"{name}::{sheet_name}"
                results.append({"file": source_key, "result": r})

    s3.put_object(
        Bucket=BUCKET,
        Key=f"jobs/{job_id}/prevalidation.json",
        Body=json.dumps(results)
    )
    return results


@app.post("/prevalidate/{job_id}")
def run_prevalidation(job_id: str):
    return _run_prevalidation(job_id)


def _load_job_files(job_id):
    """Load all raw files for a job from S3 into memory."""
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"jobs/{job_id}/raw/")
    if "Contents" not in resp:
        return []

    files = []
    for obj in resp["Contents"]:
        key = obj["Key"]
        if key.endswith("/"):
            continue
        data = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        files.append({"name": key.split("/")[-1], "bytes": data})

    return files


def _do_mapping(job_id):
    """Core mapping logic with post-validation to catch type mismatches and duplicates."""
    files = _load_job_files(job_id)

    if not files:
        return {"error": "No files found for job"}

    logger.info(f"Mapping job {job_id}: {len(files)} files")

    registry = extract_columns(files)
    if not registry:
        return {"error": "Failed to extract columns"}

    hero_cols = load_hero_schema()
    hero_full = load_hero_schema_full()
    prompt = build_global_prompt(registry, hero_cols, hero_full=hero_full)

    logger.info(f"Prompt length: {len(prompt)} chars (~{len(prompt)//4} tokens)")

    last_error = None
    raw_text = None

    for attempt in range(3):
        try:
            used_prompt = (
                prompt
                + "\n\nCRITICAL: Output ONLY VALID MINIFIED JSON. "
                + "No text before or after. No markdown. "
                + "No trailing commas. Start with { and end with }."
            )

            txt = call_llm(used_prompt)
            raw_text = txt

            mapping = extract_json(txt)

            if not isinstance(mapping, dict):
                raise ValueError("Parsed mapping is not a dictionary")

            final = {}
            for h in hero_cols:
                if h not in mapping:
                    continue
                entry = mapping[h]
                if not isinstance(entry, dict):
                    continue
                file_val = entry.get("file")
                col = entry.get("column")
                conf = entry.get("confidence", 0)
                if conf is None:
                    conf = 0
                try:
                    conf = float(conf)
                except Exception:
                    conf = 0
                conf = max(0.0, min(1.0, conf))

                final[h] = {
                    "file": file_val,
                    "column": col,
                    "confidence": round(conf, 3),
                }

            logger.info(f"LLM mapping: {len(final)}/{len(hero_cols)} fields mapped")

            validated, validation_issues = validate_mapping(final, registry)

            if validation_issues:
                logger.warning(
                    f"Post-validation found {len(validation_issues)} issues, "
                    f"mapping reduced from {len(final)} to {len(validated)} fields"
                )
                for issue in validation_issues:
                    logger.warning(f"  [{issue['type']}] {issue['message']}")

            logger.info(f"Final mapping: {len(validated)}/{len(hero_cols)} fields after validation")

            s3.put_object(
                Bucket=BUCKET,
                Key=f"jobs/{job_id}/mapping.json",
                Body=json.dumps(validated, indent=2)
            )

            if validation_issues:
                s3.put_object(
                    Bucket=BUCKET,
                    Key=f"jobs/{job_id}/validation_issues.json",
                    Body=json.dumps(validation_issues, indent=2, default=str)
                )

            return {
                "job_id": job_id,
                "mapped_fields": len(validated),
                "total_hero_fields": len(hero_cols),
                "mapping": validated,
                "validation_issues": validation_issues,
                "fields_removed_by_validation": len(final) - len(validated),
            }

        except Exception as e:
            last_error = str(e)
            logger.warning(f"Mapping attempt {attempt+1} failed: {last_error}")
            continue

    return {
        "error": f"LLM mapping failed after 3 attempts: {last_error}",
        "raw_response": (raw_text or "")[:500]
    }


@app.post("/map/{job_id}")
def run_mapping(job_id: str):
    result = _do_mapping(job_id)
    if isinstance(result, dict) and "error" in result:
        try:
            pre = _run_prevalidation(job_id)
            fallback_mapping = _build_fallback_mapping_from_pre(pre)
            if fallback_mapping:
                s3.put_object(
                    Bucket=BUCKET,
                    Key=f"jobs/{job_id}/mapping.json",
                    Body=json.dumps(fallback_mapping, indent=2),
                )
                return {
                    "job_id": job_id,
                    "mapped_fields": len(fallback_mapping),
                    "total_hero_fields": len(load_hero_schema()),
                    "mapping": fallback_mapping,
                    "warning": "LLM mapping unavailable; used prevalidation-based fallback mapping.",
                    "llm_error": result.get("error"),
                }
        except Exception as fallback_err:
            logger.warning(f"/map fallback failed for {job_id}: {fallback_err}")
    return result


def _do_staging(job_id, mapping, files_with_df):
    """
    Build staged DataFrame by pulling mapped columns from source files.
    Handles multiple files with different row counts by using the
    largest file as the base index.
    """
    hero = load_hero_schema()

    dfs = {}
    for f in files_with_df:
        dfs[f["name"]] = f["df"]

    max_rows = max(len(df) for df in dfs.values())
    staged_df = pd.DataFrame(index=range(max_rows))

    mapped_count = 0
    skipped = []

    for h in hero:
        if h not in mapping:
            staged_df[h] = None
            continue

        src = mapping[h]
        src_file = src.get("file") if isinstance(src, dict) else None
        src_col = src.get("column") if isinstance(src, dict) else None

        if not src_file or not src_col:
            staged_df[h] = None
            skipped.append(h)
            continue

        if src_file not in dfs:
            staged_df[h] = None
            skipped.append(h)
            logger.warning(f"Staging: file '{src_file}' not found for hero field '{h}'")
            continue

        if src_col not in dfs[src_file].columns:
            staged_df[h] = None
            skipped.append(h)
            logger.warning(f"Staging: column '{src_col}' not found in '{src_file}' for hero field '{h}'")
            continue

        source_series = dfs[src_file][src_col].reset_index(drop=True)
        staged_df[h] = source_series
        mapped_count += 1

    logger.info(f"Staging: {mapped_count} columns populated, {len(skipped)} skipped")

    buf = io.StringIO()
    staged_df.to_csv(buf, index=False)
    s3.put_object(Bucket=BUCKET, Key=f"jobs/{job_id}/staging.csv", Body=buf.getvalue())

    return staged_df


def _build_fallback_mapping_from_pre(pre_results):
    """
    Build deterministic mapping from prevalidation match_details when LLM
    mapping is unavailable (e.g., Bedrock access/payment issues).
    """
    if not pre_results:
        return {}

    confidence_by_match_type = {
        "exact": 0.95,
        "normalized": 0.9,
        "synonym": 0.82,
        "partial": 0.7,
    }

    # Keep highest-confidence source when multiple sheets map to same hero field.
    best = {}
    for item in pre_results:
        source_file = item.get("file")
        result = item.get("result", {}) if isinstance(item, dict) else {}
        details = result.get("match_details", []) if isinstance(result, dict) else []
        if not source_file or not isinstance(details, list):
            continue

        for m in details:
            if not isinstance(m, dict):
                continue
            src_col = m.get("source")
            hero_col = m.get("hero")
            match_type = m.get("match_type", "").lower()
            if not src_col or not hero_col:
                continue

            conf = confidence_by_match_type.get(match_type, 0.65)
            candidate = {
                "file": source_file,
                "column": src_col,
                "confidence": conf,
            }

            if hero_col not in best or candidate["confidence"] > best[hero_col]["confidence"]:
                best[hero_col] = candidate

    # Round for consistency with LLM mapping format.
    return {
        hero: {
            "file": info["file"],
            "column": info["column"],
            "confidence": round(float(info["confidence"]), 3),
        }
        for hero, info in best.items()
    }


def generate_stage_summaries(pre, mapping, tags, timings, errors):
    """Call LLM to generate 3 bullet-point summaries per pipeline stage."""
    hero_full = load_hero_schema_full()

    mandatory_fields = [k for k, v in hero_full.items() if v.get("mandatory_column") == "Mandatory"]
    mandatory_mapped = [f for f in mandatory_fields if f in mapping]
    mandatory_missing = [f for f in mandatory_fields if f not in mapping]

    summary_data = {
        "prevalidation": {
            "files_checked": len(pre),
            "issues_found": sum(len(p.get("result", {}).get("issues", [])) for p in pre),
            "file_details": [
                {
                    "file": p.get("file"),
                    "rows": p.get("result", {}).get("rows", 0),
                    "cols": p.get("result", {}).get("cols", 0),
                    "hero_overlap": p.get("result", {}).get("hero_overlap_ratio", 0),
                    "null_ratio": p.get("result", {}).get("null_ratio", 0),
                    "issues": p.get("result", {}).get("issues", [])
                }
                for p in pre
            ],
            "time": timings.get("prevalidation", 0)
        },
        "mapping": {
            "total_mapped": len(mapping),
            "total_hero_fields": len(hero_full),
            "mandatory_mapped": len(mandatory_mapped),
            "mandatory_total": len(mandatory_fields),
            "mandatory_missing": mandatory_missing[:10],
            "high_confidence": sum(1 for v in mapping.values() if v.get("confidence", 0) >= 0.8),
            "medium_confidence": sum(1 for v in mapping.values() if 0.5 <= v.get("confidence", 0) < 0.8),
            "low_confidence": sum(1 for v in mapping.values() if v.get("confidence", 0) < 0.5),
            "files_used": list(set(v.get("file", "") for v in mapping.values() if v.get("file"))),
            "time": timings.get("mapping", 0)
        },
        "staging": {
            "time": timings.get("staging", 0),
            "errors": [e for e in errors if "Staging" in e]
        },
        "tagging": {
            "anomalies_found": len(tags),
            "anomaly_samples": tags[:5] if tags else [],
            "time": timings.get("tagging", 0)
        }
    }

    prompt = (
        "You are a data onboarding analyst.\n\n"
        "Given the following pipeline stage results, write EXACTLY 3 concise bullet points for EACH stage.\n"
        "Each bullet should be a single sentence highlighting a key insight, result, or concern.\n\n"
        "Stage Results:\n"
        + json.dumps(summary_data, indent=2, default=str)
        + '\n\nReturn ONLY valid JSON in this EXACT format:\n'
        + '{\n'
        + '  "prevalidation": ["bullet 1", "bullet 2", "bullet 3"],\n'
        + '  "mapping": ["bullet 1", "bullet 2", "bullet 3"],\n'
        + '  "staging": ["bullet 1", "bullet 2", "bullet 3"],\n'
        + '  "tagging": ["bullet 1", "bullet 2", "bullet 3"]\n'
        + '}\n\n'
        + 'Output ONLY parseable JSON. No markdown. No extra text.'
    )

    try:
        txt = call_llm(prompt, max_tokens=1500)
        return extract_json(txt)
    except Exception:
        return {}


@app.post("/run/{job_id}")
def run_pipeline(job_id: str):
    timings = {}
    errors = []
    warnings = []

    # PREVALIDATION
    try:
        t0 = time.time()
        pre = _run_prevalidation(job_id)
        timings["prevalidation"] = round(time.time() - t0, 2)
    except Exception as e:
        errors.append(f"Prevalidation failed: {str(e)}")
        pre = []

    # MAPPING
    validation_issues = []
    try:
        t1 = time.time()
        map_response = _do_mapping(job_id)

        if isinstance(map_response, dict) and "error" in map_response:
            raise ValueError(map_response["error"])

        if isinstance(map_response, dict) and "mapping" in map_response:
            mapping = map_response["mapping"]
        else:
            mapping = {}

        validation_issues = map_response.get("validation_issues", [])

        timings["mapping"] = round(time.time() - t1, 2)
    except Exception as e:
        mapping_error = str(e)
        mapping = _build_fallback_mapping_from_pre(pre)
        if mapping:
            logger.warning(
                f"Using fallback mapping from prevalidation: {len(mapping)} fields mapped"
            )
            warnings.append(
                "LLM mapping unavailable; used prevalidation-based fallback mapping."
            )
            try:
                s3.put_object(
                    Bucket=BUCKET,
                    Key=f"jobs/{job_id}/mapping.json",
                    Body=json.dumps(mapping, indent=2),
                )
            except Exception as persist_err:
                logger.warning(f"Failed to persist fallback mapping: {persist_err}")
                errors.append(f"Mapping fallback persist failed: {persist_err}")
        else:
            errors.append(f"Mapping failed: {mapping_error}")
            mapping = {}

    # LOAD FILES (all files and all Excel sheets)
    try:
        objs = list_raw_files(job_id)
        files_with_df = []
        for obj in objs:
            key = obj["Key"]
            data = load_file(key)
            name = key.split("/")[-1]
            for source_key, df in read_all_sheets(data, name):
                files_with_df.append({"name": source_key, "df": df})
        logger.info(f"Loaded {len(files_with_df)} file/sheet(s) for staging")
    except Exception as e:
        errors.append(f"File loading failed: {str(e)}")
        files_with_df = []

    # STAGING
    staged_df = None
    try:
        t2 = time.time()
        if not files_with_df or not mapping:
            raise ValueError("Missing files or mapping")

        staged_df = _do_staging(job_id, mapping, files_with_df)
        timings["staging"] = round(time.time() - t2, 2)
    except Exception as e:
        errors.append(f"Staging failed: {str(e)}")

    # TAGGING
    try:
        t3 = time.time()
        if staged_df is None:
            raise ValueError("No staged data")
        tags = detect(staged_df)
        timings["tagging"] = round(time.time() - t3, 2)
    except Exception as e:
        errors.append(f"Tagging failed: {str(e)}")
        tags = []

    # SUMMARY + AI INSIGHTS
    try:
        summary = build_summary(pre, mapping, tags, timings)
        summary["errors"] = errors
        summary["warnings"] = warnings
        summary["validation_issues"] = validation_issues
        summary["fields_removed_by_validation"] = len(validation_issues)
        if errors:
            summary["status"] = "failed"

        try:
            stage_summaries = generate_stage_summaries(pre, mapping, tags, timings, errors)
            summary["stage_summaries"] = stage_summaries
        except Exception:
            summary["stage_summaries"] = {}

        s3.put_object(
            Bucket=BUCKET,
            Key=f"jobs/{job_id}/summary.json",
            Body=json.dumps(summary)
        )
    except Exception as e:
        summary = {
            "job_id": job_id,
            "status": "system_error",
            "errors": errors + [str(e)],
            "timings": timings,
            "stage_summaries": {},
            "validation_issues": validation_issues,
        }

    return summary


@app.get("/preview/{job_id}")
def get_preview(job_id: str):
    """
    Return a structured preview of the mapping: source columns alongside
    the hero format columns they were mapped to, with sample data from both.
    """
    hero_full = load_hero_schema_full()
    hero_cols = load_hero_schema()

    # Load mapping
    try:
        mapping_obj = s3.get_object(Bucket=BUCKET, Key=f"jobs/{job_id}/mapping.json")
        mapping = json.loads(mapping_obj["Body"].read())
    except Exception:
        raise HTTPException(404, "Mapping not found. Run the pipeline first.")

    # Load staged data
    staged_df = None
    try:
        staging_obj = s3.get_object(Bucket=BUCKET, Key=f"jobs/{job_id}/staging.csv")
        staged_df = pd.read_csv(io.BytesIO(staging_obj["Body"].read()))
    except Exception:
        logger.warning(f"Preview: staging.csv not found for {job_id}")

    # Load raw source files/sheets for source-side samples
    source_dfs = {}
    try:
        raw_files = _load_job_files(job_id)
        for f in raw_files:
            name = f["name"]
            data = f["bytes"]
            for source_key, df in read_all_sheets(data, name):
                source_dfs[source_key] = df
    except Exception:
        logger.warning(f"Preview: could not load raw files for {job_id}")

    # Build the preview rows
    mapped_columns = []
    for hero_field, info in mapping.items():
        if not isinstance(info, dict):
            continue

        src_file = info.get("file")
        src_col = info.get("column")
        confidence = info.get("confidence", 0)
        reason = info.get("reason", "")

        hero_meta = hero_full.get(hero_field, {})

        # Source samples
        source_samples = []
        if src_file and src_col and src_file in source_dfs:
            src_df = source_dfs[src_file]
            if src_col in src_df.columns:
                source_samples = (
                    src_df[src_col]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .loc[lambda s: s != ""]
                    .unique()[:5]
                    .tolist()
                )

        # Hero (staged) samples
        hero_samples = []
        if staged_df is not None and hero_field in staged_df.columns:
            hero_samples = (
                staged_df[hero_field]
                .dropna()
                .astype(str)
                .str.strip()
                .loc[lambda s: s != ""]
                .unique()[:5]
                .tolist()
            )

        mapped_columns.append({
            "hero_field": hero_field,
            "hero_description": hero_meta.get("description", ""),
            "mandatory": hero_meta.get("mandatory_column", "Optional"),
            "source_file": src_file,
            "source_column": src_col,
            "confidence": confidence,
            "reason": reason,
            "source_samples": source_samples,
            "hero_samples": hero_samples,
        })

    # Sort: mandatory first, then by confidence desc
    mapped_columns.sort(
        key=lambda x: (0 if x["mandatory"] == "Mandatory" else 1, -x["confidence"])
    )

    # Unmapped hero columns
    unmapped_columns = []
    for col in hero_cols:
        if col not in mapping:
            meta = hero_full.get(col, {})
            unmapped_columns.append({
                "hero_field": col,
                "hero_description": meta.get("description", ""),
                "mandatory": meta.get("mandatory_column", "Optional"),
            })

    total_rows = 0
    if staged_df is not None:
        total_rows = len(staged_df)

    return {
        "job_id": job_id,
        "total_hero_fields": len(hero_cols),
        "total_mapped": len(mapped_columns),
        "total_unmapped": len(unmapped_columns),
        "coverage_pct": round(len(mapped_columns) / max(len(hero_cols), 1) * 100, 1),
        "total_rows": total_rows,
        "mapped_columns": mapped_columns,
        "unmapped_columns": unmapped_columns,
    }


@app.get("/download/{job_id}")
def download_staged(job_id: str, format: str = "xlsx"):
    """Download the staged hero-format file."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"jobs/{job_id}/staging.csv")
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    except Exception:
        raise HTTPException(404, "Staged data not found. Run the pipeline first.")

    if format == "csv":
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        content = buf.getvalue().encode("utf-8")
        media_type = "text/csv"
        filename = f"{job_id}_hero_format.csv"
    else:
        buf = io.BytesIO()
        df.to_excel(buf, index=False, engine="openpyxl")
        content = buf.getvalue()
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        filename = f"{job_id}_hero_format.xlsx"

    return StreamingResponse(
        io.BytesIO(content),
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/to-sql/{job_id}")
def hero_to_sql(job_id: str):
    """Ingest the staged Hero Format CSV into normalized SQL tables."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"jobs/{job_id}/staging.csv")
        staged_df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    except Exception:
        raise HTTPException(404, "Staged data not found. Run the pipeline first.")

    try:
        counts = ingest_hero_to_sql(job_id, staged_df)
    except Exception as e:
        logger.error(f"SQL ingestion failed for {job_id}: {e}")
        raise HTTPException(500, f"SQL ingestion failed: {str(e)}")

    return {
        "job_id": job_id,
        "status": "completed",
        "records": counts,
    }


@app.get("/sql-summary/{job_id}")
def sql_summary(job_id: str):
    """Return counts of records per table for a given job."""
    db = SessionLocal()
    try:
        table_models = {
            "facilities": Facility,
            "spaces": Space,
            "tenants": Tenant,
            "leases": Lease,
            "alternate_contacts": AlternateContact,
            "financial_balances": FinancialBalance,
            "insurance_coverages": InsuranceCoverage,
            "liens": Lien,
            "promotions": Promotion,
            "discounts": Discount,
            "military_details": MilitaryDetail,
        }

        counts = {}
        for name, model in table_models.items():
            if hasattr(model, "job_id"):
                counts[name] = db.query(model).filter(model.job_id == job_id).count()
            else:
                fk_via_tenant = name in ("alternate_contacts", "military_details")
                fk_via_lease = name in (
                    "financial_balances", "insurance_coverages", "liens",
                    "promotions", "discounts",
                )
                if fk_via_tenant:
                    tenant_ids = [
                        t.id for t in db.query(Tenant.id).filter(Tenant.job_id == job_id).all()
                    ]
                    counts[name] = db.query(model).filter(
                        model.tenant_id.in_(tenant_ids)
                    ).count() if tenant_ids else 0
                elif fk_via_lease:
                    lease_ids = [
                        l.id for l in db.query(Lease.id).filter(Lease.job_id == job_id).all()
                    ]
                    counts[name] = db.query(model).filter(
                        model.lease_id.in_(lease_ids)
                    ).count() if lease_ids else 0
                else:
                    counts[name] = 0

        total = sum(counts.values())
        return {
            "job_id": job_id,
            "total_records": total,
            "records": counts,
        }
    finally:
        db.close()


SQL_TABLE_MODELS = {
    "facilities": Facility,
    "spaces": Space,
    "tenants": Tenant,
    "leases": Lease,
    "alternate_contacts": AlternateContact,
    "financial_balances": FinancialBalance,
    "insurance_coverages": InsuranceCoverage,
    "liens": Lien,
    "promotions": Promotion,
    "discounts": Discount,
    "military_details": MilitaryDetail,
}


def _serialize_value(value):
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    return value


def _apply_job_filter(query, model, table_name, job_id, db):
    """Apply job-level filtering where possible."""
    if not job_id:
        return query

    if hasattr(model, "job_id"):
        return query.filter(model.job_id == job_id)

    if table_name in ("alternate_contacts", "military_details"):
        tenant_ids = [t.id for t in db.query(Tenant.id).filter(Tenant.job_id == job_id).all()]
        return query.filter(model.tenant_id.in_(tenant_ids)) if tenant_ids else query.filter(False)

    if table_name in ("financial_balances", "insurance_coverages", "liens", "promotions", "discounts"):
        lease_ids = [l.id for l in db.query(Lease.id).filter(Lease.job_id == job_id).all()]
        return query.filter(model.lease_id.in_(lease_ids)) if lease_ids else query.filter(False)

    return query


@app.get("/sql/tables")
def list_sql_tables():
    return {"tables": list(SQL_TABLE_MODELS.keys())}


@app.get("/sql-preview/{table_name}")
def sql_preview(table_name: str, limit: int = 50, offset: int = 0, job_id: str = ""):
    if table_name not in SQL_TABLE_MODELS:
        raise HTTPException(404, f"Unknown table: {table_name}")

    limit = max(1, min(limit, 200))
    offset = max(0, offset)

    model = SQL_TABLE_MODELS[table_name]
    db = SessionLocal()
    try:
        query = db.query(model)
        query = _apply_job_filter(query, model, table_name, job_id.strip(), db)

        total = query.count()
        rows = query.offset(offset).limit(limit).all()

        columns = [c.name for c in model.__table__.columns]
        data = []
        for row in rows:
            item = {}
            for col in columns:
                item[col] = _serialize_value(getattr(row, col))
            data.append(item)

        return {
            "table": table_name,
            "job_id": job_id.strip() or None,
            "total": total,
            "limit": limit,
            "offset": offset,
            "columns": columns,
            "rows": data,
        }
    finally:
        db.close()

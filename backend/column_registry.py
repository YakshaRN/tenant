import re
import pandas as pd
import io
import logging

logger = logging.getLogger("column_registry")

MAX_SAMPLES = 8

_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")
_PHONE_RE = re.compile(r"^[\d\s\(\)\-\+\.]{7,20}$")
_ZIP_RE = re.compile(r"^\d{5}(-\d{4})?$")
_BOOLEAN_VALUES = {"yes", "no", "true", "false", "y", "n", "1", "0"}


def _infer_semantic_type(col_name, series):
    """
    Analyse sample values to classify what kind of data a column holds.
    Returns one of: email, phone, person_name, date, numeric, boolean,
                     address, identifier, text
    """
    non_null = series.dropna().astype(str).str.strip().loc[lambda s: s != ""]
    if non_null.empty:
        return "empty"

    sample_vals = non_null.head(30)
    col_lower = col_name.lower().strip()

    email_hits = sample_vals.apply(lambda v: bool(_EMAIL_RE.match(v))).sum()
    if email_hits / max(len(sample_vals), 1) > 0.5:
        return "email"

    phone_hits = sample_vals.apply(
        lambda v: bool(_PHONE_RE.match(v)) and sum(c.isdigit() for c in v) >= 7
    ).sum()
    if phone_hits / max(len(sample_vals), 1) > 0.5:
        return "phone"

    bool_hits = sample_vals.apply(lambda v: v.lower() in _BOOLEAN_VALUES).sum()
    if bool_hits / max(len(sample_vals), 1) > 0.8:
        return "boolean"

    try:
        pd.to_numeric(sample_vals)
        return "numeric"
    except Exception:
        pass

    try:
        pd.to_datetime(sample_vals, format="mixed")
        date_kw = any(k in col_lower for k in ["date", "dt", "dob", "exp", "move", "start", "end", "paid"])
        if date_kw or len(sample_vals) > 5:
            return "date"
    except Exception:
        pass

    alpha_ratio = sample_vals.apply(
        lambda v: sum(c.isalpha() or c == ' ' for c in v) / max(len(v), 1)
    ).mean()
    name_kw = any(k in col_lower for k in ["name", "first", "last", "middle", "tenant", "customer", "contact"])
    avg_word_count = sample_vals.apply(lambda v: len(v.split())).mean()
    if name_kw and alpha_ratio > 0.8 and avg_word_count <= 4:
        return "person_name"

    return "text"


def _read_file_to_sheets(name, data):
    """
    Read a file into one or more (source_key, df) pairs.
    CSV: [(filename, df)].
    Excel: [(filename::Sheet1, df1), (filename::Sheet2, df2), ...] for all sheets.
    """
    out = []
    if name.lower().endswith(".csv"):
        try:
            df = pd.read_csv(io.BytesIO(data))
            out.append((name, df))
        except Exception as e:
            logger.warning(f"Failed to read CSV {name}: {e}")
        return out

    try:
        xl = pd.ExcelFile(io.BytesIO(data))
        for sheet_name in xl.sheet_names:
            df = pd.read_excel(xl, sheet_name=sheet_name)
            source_key = f"{name}::{sheet_name}"
            out.append((source_key, df))
    except Exception as e:
        logger.warning(f"Failed to read Excel {name}: {e}")
    return out


def extract_columns(files):
    """
    Build a rich registry of columns across all files and all sheets.
    CSV: one logical "file" per uploaded file. Excel: one entry per sheet (file::SheetName).
    Includes semantic type detection and more samples for better LLM mapping.
    """
    registry = []
    seen_sources = 0

    for f in files:
        name = f["name"]
        data = f["bytes"]

        sheets_or_single = _read_file_to_sheets(name, data)
        for source_key, df in sheets_or_single:
            if df.empty:
                logger.info(f"Skipping empty sheet/source: {source_key}")
                continue
            seen_sources += 1
            logger.info(f"Extracted {len(df.columns)} columns, {len(df)} rows from {source_key}")

            for col in df.columns:
                col_name = str(col).strip()
                if not col_name:
                    continue

                series = df[col]

                non_null = (
                    series.dropna().astype(str).str.strip().loc[lambda s: s != ""]
                )
                samples = list(non_null.unique()[:MAX_SAMPLES])

                null_ratio = float(series.isna().mean())
                dtype = str(series.dtype)

                is_numeric_like = False
                try:
                    pd.to_numeric(series.dropna().head(20))
                    is_numeric_like = True
                except Exception:
                    pass

                is_date_like = False
                try:
                    pd.to_datetime(series.dropna().head(20), format="mixed")
                    is_date_like = True
                except Exception:
                    pass

                semantic_type = _infer_semantic_type(col_name, series)

                registry.append({
                    "file": source_key,
                    "column": col_name,
                    "dtype": dtype,
                    "null_ratio": round(null_ratio, 3),
                    "is_numeric": is_numeric_like,
                    "is_date": is_date_like,
                    "semantic_type": semantic_type,
                    "samples": samples,
                    "distinct_count": int(non_null.nunique()),
                    "total_rows": len(df),
                })

    logger.info(f"Registry complete: {len(registry)} columns from {seen_sources} file/sheet(s)")
    return registry

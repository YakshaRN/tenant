import pandas as pd
import io
import re
import logging
from backend.hero_schema import load_hero_schema, load_hero_schema_with_descriptions

logger = logging.getLogger("prevalidate")


def _normalize(name):
    """Normalize a column name for fuzzy comparison."""
    s = str(name).lower().strip()
    s = re.sub(r"[^a-z0-9]", "", s)
    return s


# Common abbreviations and synonyms for fuzzy matching
_SYNONYMS = {
    "firstname": "firstname",
    "fname": "firstname",
    "first": "firstname",
    "lastname": "lastname",
    "lname": "lastname",
    "last": "lastname",
    "middlename": "middlename",
    "mname": "middlename",
    "address": "address",
    "addr": "address",
    "streetaddress": "address",
    "street": "address",
    "city": "city",
    "state": "state",
    "region": "state",
    "province": "state",
    "zip": "zip",
    "zipcode": "zip",
    "postalcode": "zip",
    "postal": "zip",
    "email": "email",
    "emailaddress": "email",
    "phone": "phone",
    "phonenumber": "phone",
    "cellphone": "cellphone",
    "cell": "cellphone",
    "mobile": "cellphone",
    "mobilephone": "cellphone",
    "homephone": "homephone",
    "workphone": "workphone",
    "dob": "dob",
    "birthday": "dob",
    "dateofbirth": "dob",
    "birthdate": "dob",
    "driverslicense": "dlid",
    "dlid": "dlid",
    "licensenumber": "dlid",
    "dl": "dlid",
    "rent": "rent",
    "monthlyrent": "rent",
    "rate": "rate",
    "baserate": "rate",
    "standardrate": "rate",
    "webrate": "webrate",
    "onlinerate": "webrate",
    "unit": "space",
    "unitno": "space",
    "unitnumber": "space",
    "space": "space",
    "spaceid": "space",
    "sqft": "sqft",
    "squarefeet": "sqft",
    "squarefootage": "sqft",
    "area": "sqft",
    "width": "width",
    "length": "length",
    "height": "height",
    "floor": "floor",
    "moveindate": "moveindate",
    "movein": "moveindate",
    "moveoutdate": "moveoutdate",
    "moveout": "moveoutdate",
    "accesscode": "accesscode",
    "gatecode": "accesscode",
    "balance": "balance",
    "accountcode": "accountcode",
    "accountid": "accountcode",
    "accountnumber": "accountcode",
    "acctno": "accountcode",
    "customername": "customername",
    "custname": "customername",
    "cname": "customername",
    "owner": "owner",
    "companyname": "owner",
    "company": "owner",
    "name": "name",
    "facilityname": "name",
    "sitename": "name",
    "propertyname": "name",
    "building": "building",
    "country": "country",
    "gender": "gender",
    "paidthroughdate": "paidthroughdate",
    "paidthrough": "paidthroughdate",
    "paidthru": "paidthroughdate",
    "billday": "billday",
    "billingday": "billday",
}

_HERO_SYNONYMS = {}


def _build_hero_synonym_map():
    """Map normalized hero field names to their canonical names."""
    if _HERO_SYNONYMS:
        return _HERO_SYNONYMS

    hero_cols = load_hero_schema()
    for col in hero_cols:
        norm = _normalize(col)
        _HERO_SYNONYMS[norm] = col

    return _HERO_SYNONYMS


def _fuzzy_match_ratio(source_cols):
    """
    Count how many source columns match hero fields using:
    1. Exact match
    2. Normalized match (lowercase, strip special chars)
    3. Synonym match
    Returns (matched_count, match_details)
    """
    hero_cols = load_hero_schema()
    hero_norm_map = _build_hero_synonym_map()
    hero_norm_set = set(hero_norm_map.keys())

    matched = []
    unmatched = []

    for sc in source_cols:
        sc_str = str(sc).strip()

        # 1. Exact match
        if sc_str in hero_cols:
            matched.append({"source": sc_str, "hero": sc_str, "match_type": "exact"})
            continue

        # 2. Normalized match
        sc_norm = _normalize(sc_str)
        if sc_norm in hero_norm_set:
            matched.append({"source": sc_str, "hero": hero_norm_map[sc_norm], "match_type": "normalized"})
            continue

        # 3. Synonym match
        synonym_key = _SYNONYMS.get(sc_norm)
        if synonym_key:
            for hero_norm, hero_orig in hero_norm_map.items():
                hero_syn = _SYNONYMS.get(hero_norm)
                if hero_syn and hero_syn == synonym_key:
                    matched.append({"source": sc_str, "hero": hero_orig, "match_type": "synonym"})
                    break
            else:
                unmatched.append(sc_str)
        else:
            # 4. Substring containment check
            found = False
            for hero_norm, hero_orig in hero_norm_map.items():
                if len(sc_norm) >= 3 and len(hero_norm) >= 3:
                    if sc_norm in hero_norm or hero_norm in sc_norm:
                        matched.append({"source": sc_str, "hero": hero_orig, "match_type": "partial"})
                        found = True
                        break
            if not found:
                unmatched.append(sc_str)

    return matched, unmatched


def prevalidate(data, filename, sheet_name=None):
    """
    Prevalidate one file (CSV) or one sheet of an Excel file.
    For Excel, pass sheet_name to validate that sheet; if None, first sheet is used.
    """
    if filename.lower().endswith(".csv"):
        df = pd.read_csv(io.BytesIO(data))
    else:
        xl = pd.ExcelFile(io.BytesIO(data))
        if sheet_name is not None:
            df = pd.read_excel(xl, sheet_name=sheet_name)
        else:
            df = pd.read_excel(xl, sheet_name=0)

    hero_cols = load_hero_schema()
    hero_desc = load_hero_schema_with_descriptions()
    issues = []
    warnings = []

    if df.empty:
        issues.append("File is empty — no data rows found")
        return {
            "rows": 0,
            "cols": 0,
            "hero_overlap_ratio": 0,
            "null_ratio": 0,
            "issues": issues,
            "warnings": warnings,
            "column_details": [],
            "match_details": [],
            "unmatched_columns": [],
        }

    if df.shape[1] < 3:
        issues.append(f"Only {df.shape[1]} columns found — expected at least 3")

    source_cols = [str(c).strip() for c in df.columns]

    # Fuzzy matching
    matched, unmatched = _fuzzy_match_ratio(source_cols)
    match_ratio = len(matched) / max(len(hero_cols), 1)

    if match_ratio < 0.05:
        issues.append(
            f"Very low overlap ({len(matched)}/{len(hero_cols)} fields matched). "
            "Column names differ significantly from hero schema — AI mapping will attempt semantic matching."
        )
    elif match_ratio < 0.15:
        warnings.append(
            f"Low overlap ({len(matched)}/{len(hero_cols)} fields matched). "
            "Some columns may need AI-based semantic matching."
        )

    # Null analysis
    overall_null = df.isna().mean().mean()
    if overall_null > 0.4:
        issues.append(f"High missing values: {overall_null:.0%} of all cells are null")
    elif overall_null > 0.2:
        warnings.append(f"Moderate missing values: {overall_null:.0%} of all cells are null")

    # Per-column details
    column_details = []
    for col in df.columns:
        col_name = str(col).strip()
        series = df[col]
        null_pct = float(series.isna().mean())
        unique_count = int(series.nunique())
        dtype = str(series.dtype)

        samples = (
            series.dropna()
            .astype(str)
            .str.strip()
            .loc[lambda s: s != ""]
            .unique()[:3]
        )

        column_details.append({
            "column": col_name,
            "dtype": dtype,
            "null_pct": round(null_pct * 100, 1),
            "unique_values": unique_count,
            "samples": list(samples),
        })

    # Duplicate column check
    dupes = [c for c in source_cols if source_cols.count(c) > 1]
    if dupes:
        issues.append(f"Duplicate column names found: {list(set(dupes))}")

    # Row duplicate check
    dupe_rows = int(df.duplicated().sum())
    if dupe_rows > 0:
        warnings.append(f"{dupe_rows} duplicate rows detected ({dupe_rows/len(df):.0%} of data)")

    return {
        "rows": int(df.shape[0]),
        "cols": int(df.shape[1]),
        "hero_overlap_ratio": round(match_ratio, 3),
        "matched_fields": len(matched),
        "total_hero_fields": len(hero_cols),
        "null_ratio": round(float(overall_null), 4),
        "issues": issues,
        "warnings": warnings,
        "match_details": matched,
        "unmatched_columns": unmatched,
        "column_details": column_details,
        "duplicate_rows": dupe_rows if dupe_rows > 0 else 0,
    }

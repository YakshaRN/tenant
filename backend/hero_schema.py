import json
import os
import logging

import boto3

logger = logging.getLogger("hero_schema")

_cache = {}

BUCKET = os.getenv("S3_BUCKET", "poc-onboarding-uploads")
S3_KEY = "hero_format_description.json"
LOCAL_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "hero_format_description.json")

FIELD_EXPECTED_TYPES = {
    "Owner": "text",
    "Name": "text",
    "Building": "text",
    "Space": "identifier",
    "Width": "numeric",
    "Length": "numeric",
    "Height": "numeric",
    "Rate": "numeric",
    "Web Rate": "numeric",
    "Space Size": "text",
    "Space Category": "text",
    "Space Type": "text",
    "Door Width": "numeric",
    "Door Height": "numeric",
    "Amenities": "text",
    "Sq. Ft.": "numeric",
    "Floor": "numeric",
    "First Name": "person_name",
    "Last Name": "person_name",
    "Middle Name": "person_name",
    "Account Code": "identifier",
    "Address": "address",
    "City": "text",
    "State": "text",
    "ZIP": "identifier",
    "Country": "text",
    "Email": "email",
    "Cell Phone": "phone",
    "Home Phone": "phone",
    "Work Phone": "phone",
    "Access Code": "identifier",
    "DOB": "date",
    "Gender": "text",
    "Active Military": "boolean",
    "DL Id": "identifier",
    "DL State": "text",
    "DL City": "text",
    "DL Exp Date": "date",
    "Rent": "numeric",
    "Last Rent Change Date": "date",
    "Move In Date": "date",
    "Move Out Date": "date",
    "Paid Date": "date",
    "Bill Day": "numeric",
    "Paid Through Date": "date",
    "Alt First Name": "person_name",
    "Alt Last Name": "person_name",
    "Alt Middle Name": "person_name",
    "Alt Address": "address",
    "Alt City": "text",
    "Alt State": "text",
    "Alt ZIP": "identifier",
    "Alt Email": "email",
    "Alt Home Phone": "phone",
    "Alt Work Phone": "phone",
    "Alt Cell Phone": "phone",
    "Security Deposit": "numeric",
    "Security Deposit Balance": "numeric",
    "Rent Balance": "numeric",
    "Fees Balance": "numeric",
    "Protection/Insurance Balance": "numeric",
    "Merchandise Balance": "numeric",
    "Late Fees Balance": "numeric",
    "Lien Fees Balance": "numeric",
    "Tax Balance": "numeric",
    "Prepaid Rent": "numeric",
    "Prepaid Additional Rent/Premium": "numeric",
    "Prepaid Tax": "numeric",
    "Protection/Insurance Provider": "text",
    "Protection/Insurance Coverage": "numeric",
    "Additional Rent/Premium": "numeric",
    "Delinquency Status": "text",
    "Lien Status": "text",
    "Lien Posted Date": "date",
    "Promotion": "text",
    "Promotion Type": "text",
    "Promotion Value": "numeric",
    "Promotion Start": "date",
    "Promotion Length": "numeric",
    "Discount": "text",
    "Discount Type": "text",
    "Discount Value": "numeric",
    "Lien Holder First Name": "person_name",
    "Lien Holder Last name": "person_name",
    "Lien Holder Email": "email",
    "Lien Holder Phone": "phone",
    "Lien Holder Address 1": "address",
    "Lien Holder Address 2": "address",
    "Lien Holder City": "text",
    "Lien Holder State": "text",
    "Lien Holder Zipcode": "identifier",
    "Commanding Officer First Name": "person_name",
    "Commanding Officer Last Name": "person_name",
    "Commanding Officer Phone": "phone",
    "Commanding Officer Email": "email",
    "Rank": "text",
    "Military Serial Number": "identifier",
    "Military Email": "email",
    "Service Member DOB": "date",
    "Expiration Term of Service": "date",
    "Military Branch": "text",
    "Military Unit Name": "text",
    "Military Unit Phone": "phone",
    "Military Unit Address 1": "address",
    "Military Unit Address 2": "address",
    "Military City": "text",
    "Military Unit State": "text",
    "Military Unit Zipcode": "identifier",
    "payment_cycle": "text",
    "IsBusinessLease": "boolean",
    "Catch Flag": "text",
    "Alarm Enabled": "boolean",
    "24-hour access": "boolean",
    "start_date": "date",
    "pay_by_date": "date",
    "end_date": "date",
    "PaperlessBilling": "boolean",
    "Offline": "boolean",
    "OfflineReason": "text",
}


def _load_raw():
    if "raw" in _cache:
        return _cache["raw"]

    if os.path.exists(LOCAL_PATH):
        logger.info(f"Loading hero schema from local file: {LOCAL_PATH}")
        with open(LOCAL_PATH, "r") as f:
            data = json.load(f)
    else:
        logger.info(f"Loading hero schema from S3: s3://{BUCKET}/{S3_KEY}")
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=BUCKET, Key=S3_KEY)
        data = json.loads(obj["Body"].read())

    _cache["raw"] = data
    return data


def load_hero_schema():
    """Return list of hero column names."""
    data = _load_raw()
    return [item["column_name"] for item in data]


def load_hero_schema_with_descriptions():
    """Return dict of {column_name: description}."""
    data = _load_raw()
    return {item["column_name"]: item.get("description", "") for item in data}


def load_hero_schema_full():
    """Return dict of {column_name: {description, mandatory_column, expected_type, ...}}."""
    data = _load_raw()
    result = {}
    for item in data:
        name = item["column_name"]
        result[name] = {
            "description": item.get("description", ""),
            "mandatory_column": item.get("mandatory_column", "Optional"),
            "mandatory_data_without_tenant": item.get("mandatory_data_without_tenant", "Optional"),
            "mandatory_data_with_tenant": item.get("mandatory_data_with_tenant", "Optional"),
            "expected_type": FIELD_EXPECTED_TYPES.get(name, "text"),
        }
    return result

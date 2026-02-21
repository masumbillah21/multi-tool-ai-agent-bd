import re

import pandas as pd
from datasets import load_dataset

CANONICAL_COLUMN_ALIASES = {
    "institutions": {
        "name": ["name", "institution_name", "institute_name", "title"],
        "institution_type": ["type", "institution_type", "category"],
        "location": ["location", "address", "area"],
        "district": ["district", "zilla"],
        "division": ["division"],
        "upazila": ["upazila", "thana", "sub_district"],
        "ownership_type": ["ownership", "ownership_type", "public_private", "management_type"],
        "contact_phone": ["phone", "phone_number", "mobile", "contact", "contact_phone"],
        "contact_email": ["email", "contact_email", "mail"],
        "website": ["website", "web", "url"],
        "established_year": ["established", "established_year", "founded", "foundation_year"],
        "student_capacity": ["capacity", "student_capacity", "seats", "intake_capacity"],
    },
    "hospitals": {
        "name": ["name", "hospital_name", "clinic_name"],
        "hospital_type": ["type", "hospital_type", "facility_type", "category"],
        "location": ["location", "address", "area"],
        "district": ["district", "zilla"],
        "division": ["division"],
        "upazila": ["upazila", "thana", "sub_district"],
        "bed_capacity": ["beds", "bed", "bed_capacity", "number_of_beds", "total_beds"],
        "doctors_count": ["doctor", "doctors", "doctor_count", "doctors_count", "number_of_doctors"],
        "nurses_count": ["nurse", "nurses", "nurse_count", "nurses_count", "number_of_nurses"],
        "ownership_type": ["ownership", "ownership_type", "public_private", "management_type"],
        "facilities": ["facilities", "services", "service_details", "specialties"],
        "contact_phone": ["phone", "phone_number", "mobile", "contact", "contact_phone"],
        "contact_email": ["email", "contact_email", "mail"],
        "website": ["website", "web", "url"],
    },
    "restaurants": {
        "name": ["name", "restaurant_name", "title"],
        "cuisine": ["cuisine", "food_type", "dish_type", "speciality"],
        "location": ["location", "address", "area"],
        "district": ["district", "zilla"],
        "division": ["division"],
        "city": ["city", "town"],
        "rating": ["rating", "score", "review_score", "avg_rating"],
        "price_range": ["price_range", "price", "budget", "cost_level"],
        "contact_phone": ["phone", "phone_number", "mobile", "contact", "contact_phone"],
        "website": ["website", "web", "url"],
        "latitude": ["latitude", "lat"],
        "longitude": ["longitude", "lng", "lon"],
    },
}

NUMERIC_HINTS = {
    "institutions": {"established_year": "int", "student_capacity": "int"},
    "hospitals": {"bed_capacity": "int", "doctors_count": "int", "nurses_count": "int"},
    "restaurants": {"rating": "float", "latitude": "float", "longitude": "float"},
}


def normalize_column_name(name: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower()).strip("_")
    if not cleaned:
        cleaned = "column"
    if cleaned[0].isdigit():
        cleaned = f"col_{cleaned}"
    return cleaned


def deduplicate_columns(columns: list[str]) -> list[str]:
    counts: dict[str, int] = {}
    out: list[str] = []
    for col in columns:
        if col not in counts:
            counts[col] = 0
            out.append(col)
        else:
            counts[col] += 1
            out.append(f"{col}_{counts[col]}")
    return out


def convert_possible_numeric(series: pd.Series) -> pd.Series:
    if series.dtype != "object":
        return series
    replaced = series.astype(str).str.replace(",", "", regex=False).str.strip()
    numeric = pd.to_numeric(replaced, errors="coerce")
    if numeric.notna().mean() >= 0.9:
        return numeric
    return series


def load_hf_to_dataframe(dataset_id: str) -> pd.DataFrame:
    ds = load_dataset(dataset_id)
    split_name = next(iter(ds.keys()))
    return ds[split_name].to_pandas()


def _match_alias(raw_col: str, aliases: list[str]) -> bool:
    raw_tokens = set(raw_col.split("_"))
    for alias in aliases:
        alias_norm = normalize_column_name(alias)
        if raw_col == alias_norm or alias_norm in raw_col or raw_col in alias_norm:
            return True
        alias_tokens = set(alias_norm.split("_"))
        if alias_tokens.issubset(raw_tokens):
            return True
    return False


def _rename_with_canonical_map(columns: list[str], table_name: str) -> list[str]:
    aliases = CANONICAL_COLUMN_ALIASES.get(table_name, {})
    renamed: list[str] = []
    for col in columns:
        new_name = col
        for canonical_name, alias_list in aliases.items():
            if _match_alias(col, alias_list):
                new_name = canonical_name
                break
        renamed.append(new_name)
    return deduplicate_columns(renamed)


def _apply_numeric_hints(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    hints = NUMERIC_HINTS.get(table_name, {})
    out = df.copy()
    for col, hint in hints.items():
        if col not in out.columns:
            continue
        numeric = pd.to_numeric(out[col].astype(str).str.replace(",", "", regex=False).str.strip(), errors="coerce")
        if numeric.notna().mean() >= 0.5:
            if hint == "int":
                out[col] = numeric.round(0).astype("Int64")
            else:
                out[col] = numeric
    return out


def _apply_schema_types(df: pd.DataFrame, column_types: dict[str, str]) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        target_type = column_types.get(col, "TEXT").upper()
        if target_type == "INTEGER":
            numeric = pd.to_numeric(out[col].astype(str).str.replace(",", "", regex=False).str.strip(), errors="coerce")
            out[col] = numeric.round(0).astype("Int64")
        elif target_type == "REAL":
            out[col] = pd.to_numeric(
                out[col].astype(str).str.replace(",", "", regex=False).str.strip(),
                errors="coerce",
            )
        # TEXT columns are intentionally preserved as-is.
    return out


def transform_dataframe(
    df: pd.DataFrame,
    table_name: str,
    column_types: dict[str, str] | None = None,
) -> pd.DataFrame:
    normalized_cols = [normalize_column_name(c) for c in df.columns]
    out = df.copy()
    out.columns = _rename_with_canonical_map(normalized_cols, table_name)
    if column_types:
        return _apply_schema_types(out, column_types)
    for col in out.columns:
        out[col] = convert_possible_numeric(out[col])
    return _apply_numeric_hints(out, table_name)

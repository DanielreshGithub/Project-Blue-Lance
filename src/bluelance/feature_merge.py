from __future__ import annotations

from pathlib import Path
import pandas as pd

PROCESSED = Path("data/processed")
INTERIM = Path("data/interim")

ACLED_FILE = PROCESSED / "acled_global_weekly_features.csv"
GDELT_FILE = INTERIM / "gdelt_country_week_features.csv"
MERGED_FILE = PROCESSED / "acled_gdelt_weekly_features_8w.csv"


# ============================================================
# (A) OPTIONAL: Country name harmonization map
# Add renames here if ACLED and GDELT use different spellings.
# You ONLY add entries once you see them in the missing-country printout.
# ============================================================
COUNTRY_RENAMES = {
    # Example patterns (fill based on your actual missing list):
    # "Cote d'Ivoire": "Côte d’Ivoire",
    # "Ivory Coast": "Côte d’Ivoire",
    # "DRC": "Democratic Republic of Congo",
    # "Congo (Democratic Republic)": "Democratic Republic of Congo",
}


def _normalize_country(s: pd.Series) -> pd.Series:
    # Basic cleanup
    s = s.astype(str).str.strip()

    # ============================================================
    # (B) OPTIONAL: Apply COUNTRY_RENAMES here
    # This is the correct spot because it affects BOTH datasets.
    # ============================================================
    if COUNTRY_RENAMES:
        s = s.replace(COUNTRY_RENAMES)

    return s


def _dedupe_gdelt(gdelt: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure exactly 1 row per (country, week).

    Current behavior: aggregates gdelt_* columns using sum.
    That is OK if duplicates represent partial chunks you want totaled.

    If duplicates are caused by accidentally appending a "fixed" row later
    (like Iran/Iraq/Mali/Peru), then KEEP-LAST is usually safer.
    """

    gdelt = gdelt.copy()

    gdelt["country"] = _normalize_country(gdelt["country"])
    gdelt["week"] = pd.to_datetime(gdelt["week"], errors="coerce")
    gdelt = gdelt.dropna(subset=["country", "week"])

    metric_cols = [c for c in gdelt.columns if c.startswith("gdelt_")]
    if not metric_cols:
        raise ValueError("No gdelt_* columns found in GDELT file.")

    # numeric + fill
    for c in metric_cols:
        gdelt[c] = pd.to_numeric(gdelt[c], errors="coerce").fillna(0)

    # diagnostics
    dup_mask = gdelt.duplicated(subset=["country", "week"], keep=False)
    if dup_mask.any():
        print("⚠️  GDELT has duplicate (country, week) keys.")
        print(gdelt.loc[dup_mask, ["country", "week"]].value_counts().head(15))

    # ============================================================
    # (C) CHOOSE ONE DEDUPE STRATEGY:
    #
    # Strategy 1 (your current): SUM duplicates
    #   Use this if duplicates represent pieces that should be totaled.
    #
    # Strategy 2 (recommended for your case): KEEP LAST row
    #   Use this if duplicates happen because you appended corrected rows.
    # ============================================================

    USE_KEEP_LAST = True

    if USE_KEEP_LAST:
        # Keep the last row for each (country, week) in file order
        # This is usually correct when "fill_missing" appended fixed rows later.
        gdelt_agg = gdelt.drop_duplicates(subset=["country", "week"], keep="last")
        gdelt_agg = gdelt_agg[["country", "week", *metric_cols]].copy()
    else:
        # Sum duplicates
        gdelt_agg = gdelt.groupby(["country", "week"], as_index=False)[metric_cols].sum()

    # sanity
    if gdelt_agg.duplicated(subset=["country", "week"]).any():
        raise RuntimeError("GDELT still duplicated after dedupe (unexpected).")

    return gdelt_agg


def main() -> None:
    if not ACLED_FILE.exists():
        raise FileNotFoundError(f"Missing ACLED file: {ACLED_FILE}")
    if not GDELT_FILE.exists():
        raise FileNotFoundError(f"Missing GDELT file: {GDELT_FILE}")

    acled = pd.read_csv(ACLED_FILE, parse_dates=["week"])
    gdelt_raw = pd.read_csv(GDELT_FILE)

    # normalize ACLED
    acled["country"] = _normalize_country(acled["country"])

    # dedupe/aggregate GDELT to 1 row per (country, week)
    gdelt = _dedupe_gdelt(gdelt_raw)

    # keep only ACLED rows for weeks present in GDELT
    weeks = sorted(gdelt["week"].unique())
    acled_8w = acled[acled["week"].isin(weeks)].copy()

    print("ACLED total:", len(acled))
    print("ACLED 8w:", len(acled_8w))
    print("GDELT rows (raw):", len(gdelt_raw))
    print("GDELT rows (deduped):", len(gdelt), "| weeks:", len(weeks))

    merged = acled_8w.merge(
        gdelt,
        on=["country", "week"],
        how="left",
        validate="m:1",
    )

    print("Merged:", len(merged))

    # ============================================================
    # (D) Diagnostics: print what’s missing (this tells you what to put in COUNTRY_RENAMES)
    # ============================================================
    if "gdelt_violence_count_30d" in merged.columns:
        missing_n = int(merged["gdelt_violence_count_30d"].isna().sum())
        print("Missing GDELT after merge:", missing_n)

        if missing_n > 0:
            miss = merged[merged["gdelt_violence_count_30d"].isna()]
            print("\nTop missing countries:")
            print(miss["country"].value_counts().head(25))

            print("\nMissing weeks:")
            print(miss["week"].value_counts().sort_index())

    MERGED_FILE.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(MERGED_FILE, index=False)
    print("Saved ->", MERGED_FILE)


if __name__ == "__main__":
    main()
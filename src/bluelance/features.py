from pathlib import Path
import pandas as pd

PROCESSED = Path("data/processed")
PROCESSED.mkdir(parents=True, exist_ok=True)

def main():
    infile = PROCESSED / "acled_global_weekly_clean.csv"
    df = pd.read_csv(infile, parse_dates=["week"])

    # 1) Collapse to ONE row per region-week
    weekly = (
        df.groupby(["country", "admin1", "week"], as_index=False)
          .agg(
              total_events=("events", "sum"),
              total_fatalities=("fatalities", "sum"),
              population_exposure=("population_exposure", "max"),
              centroid_latitude=("centroid_latitude", "mean"),
              centroid_longitude=("centroid_longitude", "mean"),
          )
    )

    # Sort for time-based features
    weekly = weekly.sort_values(["country", "admin1", "week"]).reset_index(drop=True)
    g = weekly.groupby(["country", "admin1"])

    # 2) Rolling features from PAST 4 weeks (exclude current week)
    weekly["events_4w_sum"] = g["total_events"].transform(
        lambda s: s.shift(1).rolling(window=4, min_periods=1).sum()
    )
    weekly["fatalities_4w_sum"] = g["total_fatalities"].transform(
        lambda s: s.shift(1).rolling(window=4, min_periods=1).sum()
    )
    weekly[["events_4w_sum", "fatalities_4w_sum"]] = weekly[
        ["events_4w_sum", "fatalities_4w_sum"]
    ].fillna(0)

    # 3) Target = next week fatalities
    # NOTE: last week for each (country, admin1) will be NaN — keep it for prediction
    weekly["fatalities_next_week"] = g["total_fatalities"].shift(-1)

    # 4) Convert next-week fatalities into severity labels + numeric bands
    # If fatalities_next_week is NaN, keep severity as NA (unknown yet)
    def band_fatalities(x):
        if pd.isna(x):
            return pd.NA
        if x == 0:
            return "none"
        elif 1 <= x <= 5:
            return "low"
        elif 6 <= x <= 20:
            return "medium"
        else:
            return "high"

    weekly["severity_label_next_week"] = weekly["fatalities_next_week"].apply(band_fatalities)

    band_map = {"none": 0, "low": 1, "medium": 2, "high": 3}
    weekly["severity_band_next_week"] = weekly["severity_label_next_week"].map(band_map)

    out = PROCESSED / "acled_global_weekly_features.csv"
    weekly.to_csv(out, index=False)

    print(f"Feature engineered rows: {len(weekly):,}")
    print(f"Saved -> {out}")

    # sanity
    print("Week max:", weekly["week"].max().date())
    newest = weekly[weekly["week"] == weekly["week"].max()]
    missing_targets = int(newest["fatalities_next_week"].isna().sum())
    print(f"Newest-week rows: {len(newest):,} | targets missing (expected): {missing_targets:,}")

if __name__ == "__main__":
    main()
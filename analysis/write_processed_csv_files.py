# Usage: analysis/write_processed_csv_files.py
# Options:
# --output-dir [path]  Specify output directory
# --test [test]   Choose pathology test e.g. chol
# --light   Imports light version of measures dataset (1 year)
# --sim     Generates simulated, non-zero data for local development testing

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from config import codelists


BASE_DIR = Path(__file__).parents[1]


def get_demographic_table(df_measure_output):
    """
    Get the population stratified table for a given measure
    args:
    df_measure_output: pd.DataFrame
        The dataframe containing the rounded & redacted output from the generate-measures action
    returns:
        The deciles table for the measure as a pd.DataFrame
    """
    demograph_strata = ["by_IMD", "by_ethnicity", "by_sex", "by_region"]
    df_demograph = df_measure_output[
        df_measure_output["measure"].isin(demograph_strata)
    ]

   # Remove IMD == "unknown" ONLY for by_IMD rows
    df_demograph = df_demograph.loc[
        ~(
            (df_demograph["measure"] == "by_IMD")
            & df_demograph["IMD"].fillna("").astype(str).str.strip().str.lower().eq("unknown")
        )
    ]

    df_demograph = df_demograph[
        [
            "measure",
            "interval_start",
            "ratio",
            "numerator",
            "denominator",
            "IMD",
            "ethnicity",
            "sex",
            "region",
        ]
    ]
    return df_demograph


def get_deciles_table(df_measure_output):
    """
    Get the deciles table for a given measure.
    args:
    df_measure_output: pd.DataFrame
        The dataframe containing the rounded & redacted output from the generate-measures action
    returns:
        The deciles table for the measure as a pd.DataFrame
    """
    percentiles = pd.Series(
        [*range(10, 100, 10)],
    )

    df_practice = df_measure_output.loc[
        df_measure_output["measure"] == "by_practice",
        ["interval_start", "ratio", "practice"],
    ]

    # Remove practices that have zero events during the study period
    df_practice = df_practice.loc[~df_practice["ratio"].isna()]

    # Calculate number of unique practices per month
    df_prac_per_month = (
        df_practice.groupby("interval_start")["practice"]
        .nunique()
        .reset_index(name="n_practices")
    )

    df_quantiles = (
        df_practice.groupby("interval_start")["ratio"]
        .quantile(percentiles / 100, interpolation="nearest")
        .reset_index(name="value")
    )

    df_quantiles = df_quantiles.rename(
        columns={"interval_start": "date", "level_1": "percentile"}
    )
    df_quantiles["percentile"] = (df_quantiles["percentile"] * 100).astype(int)

    # Rate per 1000 patients (if we're calculating a rate and not a mean)
    if "mean" not in args.test:
        df_quantiles["value"] = df_quantiles["value"] * 1000

    return df_quantiles, df_prac_per_month


def get_event_counts_and_top_5_codes_tables(df_measure_output, codelist_path):
    """
    Get the event counts and top 5 codes tables for a given measure.
    args:
    df_measure_output: pd.DataFrame
        The dataframe containing the rounded & redacted output from the generate-measures action
    codelist_path: string
        The path of the codelist
    returns:
        The event counts table for the measure as a pd.DataFrame
        The top 5 codes table for the measure as a pd.DataFrame
    """
    codelist = pd.read_csv(
        codelist_path,
        header=0,
        names=["Code", "Description"],
        dtype={"Code": str},
    )

    # Events counts are in the numerator except for the mean measure
    if "mean" in args.test:
        events_col = "denominator"
    else:
        events_col = "numerator"

    events = (
        df_measure_output.groupby(["snomedct_code", "interval_start"])[events_col]
        .sum()
        .rename("Events")
        .rename_axis(["Code", "Date"])
    )

    # Initialize output tables
    df_event_counts = pd.DataFrame(
        columns=["count"], index=["total_events", "events_in_latest_period"]
    )
    df_code_counts = pd.DataFrame(
        columns=["Code", "Events", "Description", "Proportion of codes (%)"]
    )

    if not events.empty:
        # Populate the event counts table
        df_event_counts.loc["total_events"] = events.sum()

        grouped_by_date = events.groupby(level="Date").sum().sort_index(ascending=False)

        if not grouped_by_date.empty:
            df_event_counts.loc["events_in_latest_period"] = grouped_by_date.iloc[0]
        else:
            df_event_counts.loc["events_in_latest_period"] = 0

        # Tabulate the Top 5 codes by event count
        df_code_counts = (
            events.groupby(level="Code")
            .sum()
            .sort_values(ascending=False)
            .reset_index()
            .merge(codelist, on="Code", how="left")  # In case code not in codelist
        )

        # Calculate proportion of codes
        total_events = df_code_counts["Events"].sum()
        df_code_counts["Proportion of codes (%)"] = (
            round(100 * df_code_counts["Events"] / total_events, 2)
        ).astype(str)

        # Formatting tweaks
        if len(df_code_counts) > 1:
            df_code_counts.loc[
                df_code_counts["Proportion of codes (%)"].str.replace("0", "") == ".",
                "Proportion of codes (%)",
            ] = "< 0.005"

            df_code_counts.loc[
                df_code_counts["Proportion of codes (%)"].str.startswith("100."),
                "Proportion of codes (%)",
            ] = "> 99.995"
    else:
        df_event_counts.loc["total_events"] = 0
        df_event_counts.loc["events_in_latest_period"] = 0

    return df_event_counts, df_code_counts.iloc[:5]


def main(output_dir, codelist_path):
    """
    For a given output directory, read in the rounded & redacted measures file,
    write the deciles table, event counts table and top 5 codes table to CSV files.
    These files are SDC-compliant as they are derived from the rounded & redacted measures files.
    args:
    output_dir: pathlike
        The directory of the output
    codelist_path: string
        The path to the codelist
    """

    if args.sim:
        df = pd.read_feather(output_dir / "measures.arrow")
        df["numerator"] = np.random.randint(0, 500, size=len(df))
        df["denominator"] = np.random.randint(500, 1000, size=len(df))
        df["ratio"] = df["numerator"] / df["denominator"]
        suffix = "_sim"
    elif args.light:
        df = pd.read_feather(output_dir / "measures_light.arrow")
        suffix = "_light"
    else:
        df = pd.read_feather(output_dir / "measures.arrow")
        suffix = ""

    df["practice"] = df["practice"].astype("Int64")

    deciles_table, df_prac_per_month = get_deciles_table(df)
    deciles_table.to_csv(
        output_dir / f"deciles_table_counts_per_month{suffix}.csv",
        index=False,
    )
    df_prac_per_month.to_csv(
        output_dir / f"practice_counts_per_month{suffix}.csv",
        index=False,
    )

    # Generate demographic breakdown for original measures
    if args.test in [
        "alt",
        "chol",
        "hba1c",
        "hba1c_numeric",
        "rbc",
        "sodium",
        "systol",
    ]:
        demographic_table = get_demographic_table(df)
        demographic_table.to_csv(
            output_dir / f"demographic_table_counts_per_week{suffix}.csv",
            index=False,
        )

    event_counts, top_5_code_table = get_event_counts_and_top_5_codes_tables(
        df, codelist_path
    )
    event_counts.to_csv(
        output_dir / f"event_counts{suffix}.csv",
    )
    top_5_code_table.to_csv(
        output_dir / f"top_5_code_table{suffix}.csv",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test")
    parser.add_argument("--output-dir")
    parser.add_argument("--sim", action="store_true")
    parser.add_argument("--light", action="store_true")
    args = parser.parse_args()

    # Specify test for cases that use multiple codelists e.g. hba1c_diabetes
    if "hba1c" in args.test:
        codelist_path = codelists["hba1c_numeric"]
    elif "alt" in args.test:
        codelist_path = codelists["alt"]
    else:
        codelist_path = codelists[args.test]

    output_dir = BASE_DIR / Path(args.output_dir)
    main(output_dir, codelist_path)

#!/usr/bin/env python3
"""
DFC Data Preprocessing + BU/PA Correction Pipeline

Steps (in order):
  1.  Load and concatenate YTD + MTD CSV files.
  2.  Drop fully-empty and specified columns  -> cleaned_data1.csv
  3.  Apply business filters (SO TYPE, Project Type, Practice Area,
      Market, Project Billability Type, Cancellation Reason)  -> cleaned_data2.csv
  4.  Exclude and combine SO GRADE values (e.g. GenC merge, remove
      cont / D / VP / TD Trainee etc.).
  5.  Parse date-like columns to pl.Date.
  6.  Remove rows where Requirement Start Date year is outside 2023-2025.
  7.  Remove rows where Requirement Start Date < SO Submission Date (RSD < SSD).
  8.  Drop rows with any null value  -> DFC_YTD_..._V2.csv
  9.  Correct BU from SBU-BU mapping file (Americas market only).
  10. Convert GGM Market rows to EMEA/APJ using Market Unit column;
      then drop all APJ rows.
  11. EMEA BU corrections: drop Mobility, rename Transport-UK -> T&H-UK,
      merge UK BUs -> RCGT&H-UK, rename South Europe -> SPAI,
      split Benelux into Belux / Netherlands by SBU1.
  12. Remove BU / Country whose share is <= threshold per market,
      overall or in any individual year  -> data/demand/near_zero_removed.json
  13. Remove BU / Country not present in all years per market
      -> data/demand/special_cases_removed.json
  14. Export demand JSONs (overall + year-wise per market)
      -> data/demand/  |  Save corrected CSV -> DFC_YTD_..._V2_corrected.csv

Usage examples
--------------
# Use all defaults:
python preprocess.py

# Override individual settings with inline JSON:
python preprocess.py --grades-to-remove '["cont","C"]'
python preprocess.py --grades-to-combine '{"GenC":["PT","PAT","PA","P"],"Manager":["M","SM"]}'
python preprocess.py --filters filters.json
python preprocess.py --cols-to-keep cols_keep.json --cols-to-remove cols_drop.json

# Override file paths:
python preprocess.py --ytd-files data/YTD_2024.csv data/YTD_2025.csv --mtd-file data/MTD.csv

# Custom SBU-BU mapping file:
python preprocess.py --sbu-map data/SBU-BU Mapping.xlsx

# Disable near-zero or incomplete-year removal:
python preprocess.py --no-near-zero-removal --no-incomplete-years-removal

# Custom output directory:
python preprocess.py --output-dir output/
"""

import argparse
import json
import os
import re
import sys
import time

import polars as pl


# ---------------------------------------------------------------------------
# Defaults  (exact values from the notebook)
# ---------------------------------------------------------------------------

DEFAULT_COLS_TO_KEEP = [
    "SO Line Status", "Unique ID", "Vertical", "Practice", "SubVertical",
    "SubPractice", "BU", "Parent Customer", "Project Type",
    "Project Billability Type", "Quantity", "SO Submission Date",
    "Cancellation Reason", "Off/ On", "Geography", "Country", "City",
    "Fulfilment/Cancellation Month", "Requirement Start Date", "Market",
    "SO TYPE", "SO GRADE", "Technical Skills Required", "Requirement type",
    "Practice Area", "ServiceLine", "Original Requirement Start date",
    "Revenue potential", "SBU1", "Account ID", "Account Name",
    "Parent Customer ID", "Market Unit",
]

DEFAULT_COLS_TO_REMOVE = [
    "Department", "BusinessUnit Desc", "SBU2", "Project ID", "Project Name",
    "Action Date", "SO Submission Date 2", "Offer Created Date",
    "Offer Extended Date", "Available positions in RR", "Offer Status",
    "Offer Sub Status", "No Of Offers", "Job Opening Status", "Recruiter ID",
    "Recruiter Name", "Subcontractor Allowed by Customer",
    "Interview Required by Customer", "T&MRateCard", "Assignment Start Date",
    "Job Code", "Preferred Location 1", "Preferred Location 2",
    "Requirement End Date", "Additional Revenue", "Billability Start date",
    "INTERNAL FULFILMENT-TAT", "EXTERNAL FULFILMENT- WFM -TAT",
    "EXTERNAL FULFILMENT- TAG -TAT", "TAT(Flag dt to Interview dt)",
    "TAT(Int to Offer creation)", "TAT(Offer create to Offer approve)",
    "TAT(Offer Apprvd to Offer Extnd)", "TAT(Offer extnd -EDOJ)",
    "TAT(Exp DOJ- DOJ)", "Source category", "Cancellation Ageing",
    "Open SO Ageing", "RR Ageing", "Open SO Ageing range", "RR Ageing range",
    "CCA Service Line", "CCA Service Line Description", "Track",
    "Track Description", "Sub Track", "Sub Track Description",
    "Demand Role Code", "Demand Role Description",
    "Leadership and Prof. Dev. Comp", "Additional Skills", "Skill Family",
    "RLC", "RSC1", "Domain Skill Layer 1", "Domain Skill Layer 2",
    "Domain Skill Layer 3", "Revenue Loss Category", "Staffing Team Member",
    "Staffing Team Lead", "SoStatus", "TMP SO Status",
    "Probable Fullfilment Date", "Open Trained Associate",
    "Primary Skill Set", "Expected Date Of Joining", "Replaced Associate",
    "Customer Bill rate", "Bill rate currency", "Customer Profitability",
    "OE Approval flag", "OE Approver Date", "OE Approval Comments",
    "TSC Approval flag", "TSC Approver ID", "TSC Approver Date",
    "TSC Approval Comments", "Customer Project", "Primary State tag",
    "Secondary State tag", "status_remark", "Opportunity Status",
    "Job Description", "Revenue", "greenchannel", "Forecast Category",
    "Win Probability", "Estimated Deal close date",
    "Actual Expected Revenue Start date", "Opportunity Owner", "OwnerID",
    "Recommended for Hiring By", "Recommended for Hiring On",
    "SO Priority", "MU Priority", "iRise Status", "PE Flagged",
    "IJM Allocation", "Deflag MFR", "Deflag MFR Date", "Original TAT",
    "Approver ID", "Approver Name", "Delivery/Non-Delivery",
    "Project Classification", "Service Description", "Cluster Description",
    "Demand Unit Description",
    "Is this demand open for all Cognizant locations across India?",
    "Skills(Anchor/Supplementary)", "Assignment Staging Date",
    "SO Work Model", "State", "Order Description", "Data/Voice",
    "Active RR Status", "Staffing Team Member ", "SO Billability",
    "Cancelled BY ID", "cancellation_comments", "Owning Organization",
    "Pool ID", "Pool Name", "Associate Hired Grade",
    "Flagged for Recruitment", "When Flagged for Recruitment",
    "Technical Skills Desired", "Functional Skills",
    "Original Requirement Start date",
]

DEFAULT_FILTERS = {
    "SO TYPE": ["STA"],
    "Project Type": ["EXTN", "EXANT"],
    "Practice Area": ["Digital Engineering"], #Digital Engineering, EPS
    # "ServiceLine": ["SOFTWARE & PLATFORM ENGINEERING"],
    "Market": ["Americas", "EMEA", "GGM"],
    "Project Billability Type": ["BFD", "BTB", "BTM"],
    "Cancellation Reason": [
        "NA",
        "Project/Requirement postponed or on hold by client",
        "Opportunity Lost",
        "Alternate Transactional SO created",
        "Replace by Internal Fulfilment - Allocation",
        "Requirement staffed by client/other vendor",
        "SO Criticality Change",
        "Project Preponement",
        "Staffing Challenge",
        "Labor Market Testing Unsuccessful",
    ],
}

# Each key is the new combined label; value is the list of grades to merge.
DEFAULT_GRADES_TO_COMBINE = {
    "GenC": ["PT", "PAT", "PA", "P"],
}

# Grades to completely remove from the dataset (case-insensitive).
DEFAULT_GRADES_TO_REMOVE = ["cont", "D", "SR. DIR.", "VP", "AVP", "Admin Staff", "TD Trainee"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Explicit overrides for Practice Area names that would otherwise produce a conflicting
# abbreviation. Add entries here whenever two PAs share the same auto-generated abbreviation.
# Example: "Digital Experience" would also become "DE" (same as "Digital Engineering"),
# so override it to "DX" below.
PA_ABBREV_OVERRIDES: dict = {
    # "Digital Experience": "DX",   # uncomment / add as needed
}


def pa_abbrev(practice_area: str) -> str:
    """Return the abbreviation for a Practice Area name.

    Rules (in order):
      1. Check PA_ABBREV_OVERRIDES for an explicit mapping (collision resolution).
      2. Single word  → full word uppercased:  "EPS" → "EPS", "ADM" → "ADM"
      3. Multi-word   → first letter of each:  "Digital Engineering" → "DE"

    The override dict (PA_ABBREV_OVERRIDES) must be consulted first so that
    intentionally different abbreviations always win over the auto-generated ones.
    For example, if two PAs would both abbreviate to "DE", one of them should be
    registered in PA_ABBREV_OVERRIDES with a distinct abbreviation.
    """
    # Check explicit override table first to avoid abbreviation collisions
    if practice_area in PA_ABBREV_OVERRIDES:
        return PA_ABBREV_OVERRIDES[practice_area]
    words = practice_area.strip().split()
    # Single-word PA (e.g. "EPS") → return the whole word uppercased
    # Multi-word PA (e.g. "Digital Engineering") → take the first letter of each word
    return words[0].upper() if len(words) == 1 else "".join(w[0].upper() for w in words)


def load_json_arg(value: str, arg_name: str):
    """Parse a CLI arg that is either a JSON string or a path to a JSON file.

    This lets callers pass either a literal JSON snippet on the command line
    or point to a .json file on disk.  File-path mode is tried first so that
    a path that also happens to look like valid JSON is handled correctly.
    """
    # Prefer file path: if a file exists at that location, read it as JSON
    if os.path.isfile(value):
        with open(value, "r", encoding="utf-8") as fh:
            return json.load(fh)
    # Otherwise treat the raw string as an inline JSON literal
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        print(f"[ERROR] --{arg_name}: not a valid JSON string or file path.\n  {exc}")
        sys.exit(1)


def build_final_filename(ytd_files: list, filters: dict) -> str:
    """Build output filename dynamically.

    Pattern: DFC_YTD_{min_year}-{max_year}_{PA_initials}_V2.csv
    PA initials = first letter of each word in each Practice Area value.
      e.g. ["Digital Engineering"] -> "DE"
           ["Enterprise Platform Services"] -> "EPS"
           ["Digital Engineering", "EPS"] -> "DE_EPS"
    Years come from 4-digit numbers found in the YTD filenames.

    The year span is derived from the actual filenames (not a hard-coded constant)
    so that it automatically adjusts when new annual files are added.
    """
    # Extract all 4-digit year tokens from the YTD file basenames, then take the
    # min/max to form the inclusive year range embedded in the filename.
    years = []
    for fp in ytd_files:
        years.extend(int(y) for y in re.findall(r"\d{4}", os.path.basename(fp)))
    year_part = f"{min(years)}-{max(years)}" if years else "ALL"

    # Build abbreviation from Practice Area filter values using pa_abbrev rule:
    #   single-word PA  → full word uppercased  (EPS → EPS)
    #   multi-word PA   → first-letter initials  (Digital Engineering → DE)
    # Multiple PAs are joined with "_" (e.g. DE_EPS).
    practice_areas = filters.get("Practice Area", [])
    if practice_areas:
        initials = "_".join(pa_abbrev(pa) for pa in practice_areas)
    else:
        # No PA filter → label the file as covering all Practice Areas
        initials = "ALL"

    return f"DFC_YTD_{year_part}_{initials}_V2.csv"


def _load_single_csv(file_path: str, columns: list) -> pl.DataFrame:
    """Load one CSV file, selecting only the required columns.

    Using `columns=` at read time lets polars skip parsing unwanted columns
    entirely, which is significantly faster and more memory-efficient than
    reading all columns and dropping later — especially for wide DFC exports
    that can contain 100+ fields.

    `infer_schema_length=10_000` scans 10 000 rows before deciding each
    column's dtype, which avoids premature type inference on sparse columns.
    """
    print(f"  Loading: {file_path}")
    t0 = time.time()
    size_mb = os.path.getsize(file_path) / (1024 ** 2)
    print(f"  Size: {size_mb:.1f} MB")
    # Read only the columns we need; polars will ignore all other fields
    df = pl.read_csv(file_path, columns=columns, infer_schema_length=10_000)
    print(f"  Rows: {len(df):,} | {time.time() - t0:.1f}s")
    return df


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def step_load(args) -> pl.DataFrame:
    """Step 1 – Load and concatenate all source CSV files.

    The pipeline ingests two types of source data:
    - YTD (Year-to-Date) files: one per calendar year, contain historical SOs
      that have already been closed / fulfilled within that year.
    - MTD (Month-to-Date) file: a rolling snapshot of currently open demand.

    All files are concatenated vertically (row-wise) using polars ``how="vertical"``
    which requires identical column schemas.  Loading only the whitelisted columns
    (args.cols_to_keep) minimises memory usage before any filtering is applied.
    """
    print("\n" + "=" * 60)
    print("[STEP 1] Loading source files")

    cols = args.cols_to_keep

    # Load each YTD CSV independently, then stack them into one frame.
    # This avoids reading all YTD files into memory simultaneously.
    ytd_frames = [_load_single_csv(fp, cols) for fp in args.ytd_files]
    # pl.concat with how="vertical" appends rows — schemas must match
    df_ytd = pl.concat(ytd_frames, how="vertical")
    print(f"  Combined YTD shape: {df_ytd.shape}")

    # Load the open-demand MTD snapshot
    df_mtd = _load_single_csv(args.mtd_file, cols)

    # Combine historical YTD data with current open demand
    df = pl.concat([df_ytd, df_mtd], how="vertical")
    print(f"  Combined (YTD + MTD) shape: {df.shape}")
    return df


def step_clean_columns(
    df: pl.DataFrame,
    cols_to_remove: list,
    out_path: str,
) -> pl.DataFrame:
    """Step 2 – Drop fully-empty + specified columns → cleaned_data1.csv.

    Two categories of columns are removed:
    1. Fully-empty columns: columns where every single row is null.
       These carry no information and are detected dynamically via polars'
       ``null_count()`` — a single-pass aggregate that returns the null count
       for every column simultaneously.  A column is "fully empty" when its
       null count equals the total row count.
    2. Explicitly listed columns: columns in ``cols_to_remove`` (DEFAULT_COLS_TO_REMOVE)
       that are known to be irrelevant for the demand-forecasting use-case
       (recruitment timings, approval chains, TAT metrics, etc.).

    The two sets are merged via a set union before dropping, which also
    de-duplicates in case a column appears in both categories.
    """
    print("\n" + "=" * 60)
    print("[STEP 2] Column cleaning")

    # polars null_count() returns a 1-row DataFrame with one column per source column;
    # each value is the number of null cells in that column.  Comparing against
    # len(df) identifies columns that are entirely null across the dataset.
    null_counts = df.null_count()
    empty_cols = [c for c in df.columns if null_counts[c].item() == len(df)]
    if empty_cols:
        print(f"  Fully empty columns ({len(empty_cols)}): {empty_cols}")

    # Union of auto-detected empty columns and the explicit drop list;
    # also guard against column names that don't exist in the frame (safe subset).
    to_drop = list({*empty_cols, *[c for c in cols_to_remove if c in df.columns]})
    df_clean = df.drop(to_drop)

    print(f"  Dropped {len(to_drop)} columns. Shape: {df_clean.shape}")
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    df_clean.write_csv(out_path)
    print(f"  Saved -> {out_path}")
    return df_clean


def step_filter(
    df: pl.DataFrame,
    filters: dict,
    out_path: str,
) -> pl.DataFrame:
    """Step 3 – Apply business filters → cleaned_data2.csv.

    Each key in ``filters`` is a column name; the associated value is the list
    of allowed values for that column.  Rows whose column value is NOT in the
    allowed list are dropped.  Filters are applied sequentially (AND logic):
    a row must satisfy ALL filter conditions to survive.

    Key business filter categories (see DEFAULT_FILTERS):
    - SO TYPE: retain only "STA" (Standard Assignment) SOs.
    - Project Type: retain EXTN (extension) and EXANT (anticipated extension) only;
      excludes new projects which have different demand characteristics.
    - Practice Area: scope the dataset to the relevant PA(s) e.g. Digital Engineering.
    - Market: restrict to the geographies of interest (Americas, EMEA, GGM).
    - Project Billability Type: keep BFD / BTB / BTM; excludes non-billable demand.
    - Cancellation Reason: retain only "legitimate" cancellation reasons that
      reflect real business events, filtering out administrative / data-quality entries.

    Filters are applied in dict-insertion order; missing columns generate a
    warning instead of raising an exception to allow partial datasets.
    """
    print("\n" + "=" * 60)
    print("[STEP 3] Business filtering")

    before = len(df)
    # Apply each filter in sequence; each pass further narrows the dataset
    for col, allowed_vals in filters.items():
        if col in df.columns:
            # Keep only rows whose value for this column is in the allowed set
            df = df.filter(pl.col(col).is_in(allowed_vals))
        else:
            # Non-fatal: warn but continue so partial schemas don't abort the pipeline
            print(f"  Warning: filter column '{col}' not found in DataFrame")

    print(f"  Rows: {before:,} -> {len(df):,}  (removed {before - len(df):,})")
    df.write_csv(out_path)
    print(f"  Saved -> {out_path}")
    return df


def step_combine_grades(
    df: pl.DataFrame,
    grades_col: str,
    grades_to_combine: dict,
    grades_to_remove: list,
) -> pl.DataFrame:
    """Step 4 – Exclude grades and merge grade categories.

    grades_to_combine  : {"NewLabel": ["old1", "old2", ...], ...}
    grades_to_remove   : ["cont", ...]  (case-insensitive)

    Two operations are performed on the grade column (default: "SO GRADE"):

    1. Exclusion — rows whose grade (case-insensitive) matches any entry in
       ``grades_to_remove`` are dropped entirely.  Examples of excluded grades:
       "cont" (contractor), "VP", "AVP", "TD Trainee" — roles that are either
       externally sourced or too senior/junior for the demand model.

    2. Combination — remaining grades are relabelled according to
       ``grades_to_combine``.  The default merges PT / PAT / PA / P → "GenC"
       because those granular entry-level grades are treated as a single
       demand pool for forecasting purposes.

    The combination logic uses a polars when/then chain built programmatically.
    Each iteration wraps the previous expression in an additional when/then
    layer, so the final expression is evaluated as nested conditionals:
       when(A) then X, when(B) then Y, ..., otherwise(original_value)
    All comparisons are done in uppercase to ensure case-insensitive matching.
    """
    print("\n" + "=" * 60)
    print("[STEP 4] Grade combining")

    # Work with the grade column as a string; keep a separate uppercase alias
    # for all comparisons so the original casing is preserved where unchanged.
    src = pl.col(grades_col).cast(pl.Utf8)
    upper = src.str.to_uppercase()

    # Start with the identity expression (no change) and layer when/then clauses
    # on top for each grade group that should be merged into a new label.
    # The innermost expression is always the raw source value (unchanged fallback).
    combined_expr = src
    for new_label, old_labels in grades_to_combine.items():
        upper_labels = [v.upper() for v in old_labels]
        # Wrap the previous expression: if this batch of old grades matches,
        # replace with new_label; otherwise fall through to the previous layer.
        combined_expr = (
            pl.when(upper.is_in(upper_labels))
            .then(pl.lit(new_label))
            .otherwise(combined_expr)
        )
        print(f"  Will map {old_labels} -> '{new_label}'")

    # Remove excluded grades before applying the mapping to avoid relabelling
    # and then immediately dropping rows (order matters for row counts in audit).
    excl_upper = [v.upper() for v in grades_to_remove]
    before = len(df)
    df = df.filter(~upper.is_in(excl_upper))
    print(f"  Excluded grades {grades_to_remove}: {before - len(df):,} rows removed")

    # Apply the fully-built chained expression in a single polars pass
    df = df.with_columns(combined_expr.alias(grades_col))

    unique_grades = sorted(df[grades_col].unique().to_list())
    print(f"  Unique grades after combining: {unique_grades}")
    return df


def step_clean_dates(df: pl.DataFrame) -> pl.DataFrame:
    """Step 5 – Parse date-like columns to pl.Date.

    DFC CSV exports mix several date format conventions across years and regions
    (ISO 8601, US-style with slashes, abbreviated month names, 2-digit years, etc.).
    This step standardises all detected date columns to a native polars pl.Date
    type so that downstream date arithmetic (year extraction, comparisons) works
    correctly and efficiently.

    Column detection uses a regex on column names (date, time, month, day).
    "Fulfilment/Cancellation Month" is deliberately excluded because it stores
    a human-readable label (e.g. "Jan-2024") rather than a parseable date value.

    Cascaded format parsing strategy:
    - Start with a pl.lit(None, dtype=pl.Date) as the accumulator.
    - For each candidate format string, attempt to parse the column with
      strict=False (returns null instead of raising on unparseable values).
    - fill_null() merges the result into the accumulator so that the first
      successful parse for each row "wins" and later formats only fill remaining
      nulls.  This cascade handles mixed-format columns in a single pass.
    - Formats are ordered most-common-first to minimise unnecessary work.
    """
    print("\n" + "=" * 60)
    print("[STEP 5] Date standardisation")

    # Detect columns whose name contains date/time/month/day keywords.
    # Fulfilment/Cancellation Month is a text label, not a parseable date, so exclude it.
    date_pattern = re.compile(r"date|time|month|day", re.IGNORECASE)
    date_cols = [
        c for c in df.columns
        if date_pattern.search(c) and c != "Fulfilment/Cancellation Month"
    ]
    print(f"  Date columns detected: {date_cols}")

    # Ordered list of format strings to try; the cascade stops filling nulls
    # once a format successfully parses a value for a given row.
    fmts = [
        "%Y-%m-%d",   # ISO 8601 (most common in system exports)
        "%m-%d-%Y",   # US month-first with dashes
        "%Y/%m/%d",   # ISO with slashes
        "%m/%d/%Y",   # US month-first with slashes
        "%d-%b-%Y",   # Day-AbbrevMonth-4digit-year  (e.g. 01-Jan-2024)
        "%d-%b-%y",   # Day-AbbrevMonth-2digit-year  (e.g. 01-Jan-24)
    ]

    for col in date_cols:
        # Accumulator starts as all-null Date; each format attempt fills remaining nulls
        parsed = pl.lit(None, dtype=pl.Date)
        for fmt in fmts:
            # strict=False: parse what matches, leave unmatched rows as null
            attempt = pl.col(col).cast(pl.Utf8).str.strptime(pl.Date, fmt, strict=False)
            # Merge: where accumulator is still null, use the current attempt's result
            parsed = parsed.fill_null(attempt)
        df = df.with_columns(parsed.alias(col))
        nulls = df[col].is_null().sum()
        # Report remaining nulls after all formats tried; non-zero means unparseable values
        print(f"  {col}: {nulls} null(s) after parsing")

    return df


def step_filter_rsd_year(
    df: pl.DataFrame,
    rsd_col: str = "Requirement Start Date",
    min_year: int = 2023,
    max_year: int = 2026,
) -> pl.DataFrame:
    """Step 6 – Remove rows where RSD year is outside [min_year, max_year].

    The Requirement Start Date (RSD) indicates when the staffing requirement is
    expected to begin.  Data quality issues in DFC exports can introduce SOs with
    dates far outside the intended analysis window (e.g. years like 2019 or 2030)
    which would skew year-wise demand aggregations.

    This guardrail enforces a hard date-range boundary: only rows whose RSD falls
    within the inclusive range [min_year, max_year] are retained.  The default
    range (2023-2026) matches the YTD files; 2026 rows are kept so that
    advance-planning SOs (submitted in 2025 with RSD in Jan-Jun 2026) are
    available for the SSD guardrail floor computation.

    polars ``dt.year()`` extracts the integer year from a pl.Date column, and
    ``is_between()`` performs an inclusive range check in a single vectorised pass.
    """
    print("\n" + "=" * 60)
    print(f"[STEP 6] RSD year guardrail ({min_year} <= RSD year <= {max_year})")

    if rsd_col not in df.columns:
        print(f"  Warning: '{rsd_col}' not found – skipping.")
        return df

    before = len(df)
    # dt.year() requires the column to already be a pl.Date type (from step 5)
    df = df.filter(pl.col(rsd_col).dt.year().is_between(min_year, max_year))
    print(f"  Rows: {before:,} -> {len(df):,}  (removed {before - len(df):,})")
    return df


def step_filter_rsd_before_ssd(
    df: pl.DataFrame,
    rsd_col: str = "Requirement Start Date",
    ssd_col: str = "SO Submission Date",
) -> pl.DataFrame:
    """Step 7 – Remove rows where RSD < SSD (requirement starts before submission date).

    Temporal consistency check: an SO's Requirement Start Date must be on or after
    the date the SO was submitted.  A row where RSD < SSD is logically impossible
    (the requirement would have started before it was even raised) and indicates
    a data entry error or a date parsing artefact from step 5.

    Retaining such rows would corrupt date-range calculations and year-wise demand
    summaries, so they are dropped here rather than corrected (the error source is
    upstream and cannot be reliably inferred).

    Both columns must already be pl.Date type (converted in step 5) for the
    polars column comparison ``pl.col(rsd_col) >= pl.col(ssd_col)`` to work
    correctly.  If either column is missing the step is skipped with a warning
    so that the pipeline can still run on subsets of the schema.
    """
    print("\n" + "=" * 60)
    print("[STEP 7] RSD >= SSD guardrail")

    # Guard: both date columns must exist for the comparison to be valid
    for col in (rsd_col, ssd_col):
        if col not in df.columns:
            print(f"  Warning: '{col}' not found – skipping.")
            return df

    before = len(df)
    # Keep only rows where the requirement start is on or after the submission date
    df = df.filter(pl.col(rsd_col) >= pl.col(ssd_col))
    print(f"  Rows: {before:,} -> {len(df):,}  (removed {before - len(df):,})")
    return df


def step_correct_bu(
    df: pl.DataFrame,
    map_path: str = os.path.join("data", "SBU-BU Mapping.xlsx"),
) -> pl.DataFrame:
    """Step 9 – Correct BU from SBU-BU mapping (Americas market only).

    The BU (Business Unit) column in DFC exports can be stale or inconsistently
    named for Americas SOs.  The authoritative mapping lives in an Excel file that
    maps each SBU1 (Sub-Business Unit level 1) code to its correct BU label.

    Approach — left join on a normalised SBU key:
    1. Load the Excel mapping into a lookup dict (SBU → BU) using pandas (openpyxl
       required for .xlsx).  Pandas is imported lazily here to avoid a hard
       dependency at module import time if the xlsx file is absent.
    2. Convert the dict to a small polars DataFrame so the join stays in polars.
    3. Normalise SBU1 to lowercase and strip whitespace (creates _sbu_key temp col)
       to make the join key case-insensitive and whitespace-tolerant.
    4. Left-join: every main-frame row is preserved; rows without a mapping match
       get _bu_corrected = null.
    5. Selective overwrite: only replace BU where Market == "americas" AND a
       mapping was found (_bu_corrected is not null).  Non-Americas rows are
       untouched, and Americas rows without a mapping keep their original BU.
    6. Drop the two temporary columns (_sbu_key, _bu_corrected) to restore the
       original schema.

    This approach is scope-limited to Americas because BU naming in EMEA is
    handled separately in step 11 with bespoke rename rules.
    """
    print("\n" + "=" * 60)
    print("[STEP 9] BU correction from SBU-BU mapping (Americas only)")

    # Guard: mapping file and required columns must exist
    if not os.path.isfile(map_path):
        print(f"  Warning: mapping file '{map_path}' not found – skipping.")
        return df
    if "SBU1" not in df.columns or "BU" not in df.columns:
        print("  Warning: 'SBU1' or 'BU' column missing – skipping.")
        return df

    # pandas is only needed here for reading .xlsx; import lazily to keep
    # the polars-only runtime path fast when this step is not needed.
    import pandas as _pd
    raw = _pd.read_excel(map_path, dtype=str, engine="openpyxl")
    if "SBU" not in raw.columns or "BU" not in raw.columns:
        print("  Warning: mapping file must have 'SBU' and 'BU' columns – skipping.")
        return df

    # Build normalised dict: lowercase stripped SBU → canonical BU string
    sbu_to_bu = {
        str(k).strip().lower(): str(v).strip()
        for k, v in zip(raw["SBU"], raw["BU"])
        if _pd.notna(k) and _pd.notna(v)
    }
    print(f"  Loaded {len(sbu_to_bu):,} SBU->BU mappings from {map_path}")

    # Convert lookup dict to a small polars DataFrame for an efficient hash-join
    mapping_df = pl.DataFrame({
        "_sbu_key": list(sbu_to_bu.keys()),
        "_bu_corrected": list(sbu_to_bu.values()),
    })

    # Create the normalised join key column on the main frame
    df = df.with_columns(
        pl.col("SBU1").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("_sbu_key")
    )
    # Left join: every original row is kept; unmatched rows get null in _bu_corrected
    df = df.join(mapping_df, on="_sbu_key", how="left")

    # Only overwrite BU for Americas rows where a mapping was found;
    # all other rows retain their original BU value unchanged.
    is_americas = pl.col("Market").cast(pl.Utf8).str.strip_chars().str.to_lowercase().eq("americas")
    df = df.with_columns(
        pl.when(is_americas & pl.col("_bu_corrected").is_not_null())
        .then(pl.col("_bu_corrected"))
        .otherwise(pl.col("BU"))
        .alias("BU")
    ).drop(["_sbu_key", "_bu_corrected"])  # clean up temporary join columns

    print(f"  Americas rows processed: {df.filter(is_americas).height:,}")
    print(f"  Rows after BU correction: {len(df):,}")
    return df


def step_convert_ggm_drop_apj(df: pl.DataFrame) -> pl.DataFrame:
    """Step 10 – Convert GGM Market rows using Market Unit; drop APJ rows.

    GGM (Global Growth Markets) is an internal Cognizant umbrella market that
    spans both APJ (Asia-Pacific and Japan) and certain EMEA sub-regions.  For
    demand forecasting purposes GGM is not a standalone market: each GGM row must
    be reclassified into the appropriate leaf market (EMEA or APJ) based on its
    Market Unit sub-field, and then APJ rows are removed entirely because APJ is
    outside the current scope of the demand model.

    Conversion logic:
    - market_unit_to_market maps each Market Unit value to its target market.
    - APAC and Japan → APJ (will be dropped in the next step).
    - All European / Middle-Eastern units → EMEA (retained for analysis).
    - GGM rows with an unrecognised Market Unit receive null from map_elements
      and keep their original Market value via the second when/then layer.
    - Non-GGM rows are never touched; the conditional guards ensure only rows
      with Market == "ggm" (case-insensitive) are modified.

    Two-pass column update pattern (required because polars expressions are
    immutable — you cannot overwrite a column in the same with_columns call
    that reads it):
    1. Compute _mapped_market (null for non-GGM rows, new market for GGM rows).
    2. In a second with_columns, overwrite Market: use _mapped_market where it is
       not null, otherwise keep the original Market value.
    3. Drop the temporary _mapped_market column.

    After GGM is resolved, any row still labelled APJ (both originally-APJ and
    newly-converted APAC/Japan GGM rows) is filtered out, leaving only
    Americas and EMEA rows in the pipeline.
    """
    print("\n" + "=" * 60)
    print("[STEP 10] GGM Market conversion + APJ drop")

    if "Market" not in df.columns or "Market Unit" not in df.columns:
        print("  Warning: 'Market' or 'Market Unit' column missing – skipping.")
        return df

    # Explicit Market Unit → Market mapping table.
    # GGM rows whose Market Unit is not in this dict keep their original Market value.
    market_unit_to_market = {
        "APAC": "APJ",       "Japan": "APJ",           # → will be dropped below
        "Benelux": "EMEA",   "CE Others": "EMEA",      # → retained as EMEA
        "Central Europe": "EMEA",
        "M&A GGM": "EMEA",   "Nordics": "EMEA",
        "Southern Europe & Middle East": "EMEA",        # → retained as EMEA
        "UK&I": "EMEA",
    }

    # Case-insensitive GGM detection; strip whitespace to handle trailing spaces
    is_ggm = pl.col("Market").cast(pl.Utf8).str.strip_chars().str.to_lowercase().eq("ggm")
    ggm_count = df.filter(is_ggm).height
    print(f"  GGM rows found: {ggm_count:,}")

    # Pass 1: for GGM rows, look up the new market via map_elements (Python dict lookup
    # per row); non-GGM rows get null so they are ignored in pass 2.
    df = df.with_columns(
        pl.when(is_ggm)
        .then(
            pl.col("Market Unit").cast(pl.Utf8).str.strip_chars()
            .map_elements(lambda mu: market_unit_to_market.get(mu), return_dtype=pl.Utf8)
        )
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("_mapped_market")
    ).with_columns(
        # Pass 2: overwrite Market only where _mapped_market is not null (i.e. GGM rows
        # with a recognised Market Unit); all others keep their original Market value.
        pl.when(pl.col("_mapped_market").is_not_null())
        .then(pl.col("_mapped_market"))
        .otherwise(pl.col("Market"))
        .alias("Market")
    ).drop("_mapped_market")  # remove the temporary staging column

    # Drop all APJ rows (original APJ rows + GGM→APJ converted rows)
    before_apj = len(df)
    is_apj = pl.col("Market").cast(pl.Utf8).str.strip_chars().str.to_lowercase().eq("apj")
    df = df.filter(~is_apj)
    print(f"  APJ rows dropped: {before_apj - len(df):,}")
    print(f"  Rows after: {len(df):,}")
    return df


def step_correct_emea_bu(df: pl.DataFrame) -> pl.DataFrame:
    """Step 11 – EMEA BU corrections (Mobility drop, UK merge, South Europe rename, Benelux split).

    EMEA BU labels in DFC exports contain several inconsistencies and legacy names
    that must be normalised before the data can be used for demand analysis.  All
    operations in this step are scoped exclusively to EMEA rows (non-EMEA rows are
    never modified).  The corrections are applied in a fixed order because some
    steps produce labels that feed into subsequent ones (e.g. Transport-UK is
    renamed to T&H-UK before the UK merge step, so T&H-UK is included in the merge).

    Correction sequence:
    1. Drop Mobility (EMEA only) — Mobility is a practice that is not part of the
       DE demand scope; removing the rows rather than just the BU label keeps the
       dataset consistent with the PA filter applied in step 3.
    2. Rename Transport-UK → T&H-UK — aligns with the consolidated UK transport
       and hospitality BU naming convention used in downstream reporting.
    3. Merge UK BUs (RT&H-UK, T&H-UK, RCG-UK) → RCGT&H-UK — these three BUs are
       treated as a single reporting entity; merging here avoids fragmented slices
       that would be removed by the near-zero filter in step 12.
    4. Rename South Europe → SPAI — SPAI (Southern Europe & Middle East) is the
       canonical reporting name; the old label was inconsistently used in exports.
    5. Split Benelux by SBU1: BELGIUM → Belux, NETHERLANDS → Netherlands — Benelux
       is a combined BU that must be disaggregated for country-level demand analysis.
       The SBU1 column carries the country indicator needed for the split.

    Note: is_emea is re-evaluated after the Mobility row drop (step 1) because
    polars lazy expressions capture the column reference at build time — after
    filter() the frame's row count changes and the expression must be rebuilt to
    apply correctly in subsequent with_columns calls.
    """
    print("\n" + "=" * 60)
    print("[STEP 11] EMEA BU corrections")

    if "Market" not in df.columns or "BU" not in df.columns:
        print("  Warning: 'Market' or 'BU' column missing – skipping.")
        return df

    # Build the EMEA row selector; uppercase comparison avoids case sensitivity issues
    is_emea = pl.col("Market").cast(pl.Utf8).str.strip_chars().str.to_uppercase().eq("EMEA")
    if df.filter(is_emea).height == 0:
        print("  No EMEA rows in dataset – skipping.")
        return df

    # 1. Drop Mobility (EMEA only) — Mobility is out of DE practice scope
    n_mobility = df.filter(is_emea & pl.col("BU").eq("Mobility")).height
    df = df.filter(~(is_emea & pl.col("BU").eq("Mobility")))
    print(f"  Dropped EMEA rows with BU='Mobility': {n_mobility:,}")

    # Rebuild is_emea after the row-count change caused by the Mobility filter above
    is_emea = pl.col("Market").cast(pl.Utf8).str.strip_chars().str.to_uppercase().eq("EMEA")

    # 2. Rename Transport-UK -> T&H-UK (EMEA only)
    df = df.with_columns(
        pl.when(is_emea & pl.col("BU").eq("Transport-UK"))
        .then(pl.lit("T&H-UK")).otherwise(pl.col("BU")).alias("BU")
    )
    print("  Renamed 'Transport-UK' -> 'T&H-UK' (EMEA only)")

    # 3. Merge RT&H-UK, T&H-UK, RCG-UK -> RCGT&H-UK (EMEA only)
    uk_bus = ["RT&H-UK", "T&H-UK", "RCG-UK"]
    df = df.with_columns(
        pl.when(is_emea & pl.col("BU").is_in(uk_bus))
        .then(pl.lit("RCGT&H-UK")).otherwise(pl.col("BU")).alias("BU")
    )
    print(f"  Merged {uk_bus} -> 'RCGT&H-UK' (EMEA only)")

    # 4. Rename South Europe -> SPAI (EMEA only)
    df = df.with_columns(
        pl.when(is_emea & pl.col("BU").eq("South Europe"))
        .then(pl.lit("SPAI")).otherwise(pl.col("BU")).alias("BU")
    )
    print("  Renamed 'South Europe' -> 'SPAI' (EMEA only)")

    # 5. Split Benelux by SBU1 (EMEA only)
    if "SBU1" in df.columns:
        is_benelux = is_emea & pl.col("BU").eq("Benelux")
        sbu_upper = pl.col("SBU1").cast(pl.Utf8).str.strip_chars().str.to_uppercase()
        df = df.with_columns(
            pl.when(is_benelux & sbu_upper.eq("BELGIUM")).then(pl.lit("Belux"))
            .when(is_benelux & sbu_upper.eq("NETHERLANDS")).then(pl.lit("Netherlands"))
            .otherwise(pl.col("BU")).alias("BU")
        )
        print("  Split Benelux: BELGIUM->Belux, NETHERLANDS->Netherlands (EMEA only)")
    else:
        print("  Warning: 'SBU1' column missing – skipping Benelux split.")

    print(f"  EMEA rows after corrections: {df.filter(is_emea).height:,}")
    return df


def step_remove_near_zero(
    df: pl.DataFrame,
    near_zero_pct=None,
    out_dir: str = os.path.join("data", "demand"),
    remove: bool = True,
) -> pl.DataFrame:
    """Step 12 – Remove BU/Country whose share <= threshold per market, overall or in any year.

    A BU or Country with a very small share of total demand within its market is
    considered statistically unreliable and is excluded from the demand model to
    avoid noise in proportional analyses.  The threshold (near_zero_pct) is
    expressed as a percentage of total rows in that market.

    Default thresholds:
    - Americas: 0.0% — any BU/Country with exactly 0 rows is removed (practically
      this catches empty string / null values that survived earlier cleaning).
    - EMEA:     0.1% — BUs/Countries accounting for 0.1% or less of EMEA rows are
      removed to eliminate negligible entries that would distort percentage splits.

    The check is applied twice per market:
    1. Overall (across all years): the BU/Country's share within the full market slice.
    2. Per-year: the BU/Country's share within each individual year's market slice.
    A BU/Country is flagged for removal if it falls at or below the threshold in
    EITHER the overall count OR in any single year.  This is a conservative "worst
    case" approach — if it is near-zero even in one year, the trend analysis would
    be unreliable.

    Removal is deferred (collect all violators first, then filter once) to avoid
    modifying the frame while iterating over it, and to produce a clean audit log.
    The list of removed BUs/Countries is written to near_zero_removed.json for
    transparency and post-run validation.
    """
    print("\n" + "=" * 60)
    print("[STEP 12] Near-zero BU/Country removal")

    # Apply default market-specific thresholds if none provided
    if near_zero_pct is None:
        near_zero_pct = {"Americas": 0.0, "EMEA": 0.1}

    if not remove or "Market" not in df.columns:
        print("  Skipping (remove=False or 'Market' column missing).")
        return df

    rsd_col = "Requirement Start Date"
    has_year = rsd_col in df.columns  # year-level checks require the RSD column
    near_zero_removed = {}  # audit record: market → {BU: [...], Country: [...]}
    before = len(df)
    remove_conditions = []  # deferred: [(mkt_val, col_name, frozenset_of_values), ...]

    for mkt_label, mkt_val in [("Americas", "AMERICAS"), ("EMEA", "EMEA")]:
        mkt_expr = pl.col("Market").cast(pl.Utf8).str.strip_chars().str.to_uppercase().eq(mkt_val)
        df_mkt = df.filter(mkt_expr)
        total = len(df_mkt)
        if total == 0:
            near_zero_removed[mkt_label] = {"BU": [], "Country": []}
            continue

        # Resolve per-market threshold (supports both dict and scalar inputs)
        pct = near_zero_pct.get(mkt_label, 0.0) if isinstance(near_zero_pct, dict) else float(near_zero_pct)
        nz_bu: set = set()
        nz_country: set = set()

        # --- Overall share check (across all years for this market) ---
        for col, nz_set in [("BU", nz_bu), ("Country", nz_country)]:
            if col not in df_mkt.columns:
                continue
            # group_by + agg(pl.len()) counts rows per unique value (polars idiom for value_counts)
            vc = (
                df_mkt.select(pl.col(col).cast(pl.Utf8).str.strip_chars().fill_null("").alias(col))
                .group_by(col).agg(pl.len().alias("cnt"))
            )
            for row in vc.iter_rows(named=True):
                # Flag if percentage share (rounded to 1 dp) is at or below the threshold
                if round(row["cnt"] / total * 100, 1) <= pct:
                    nz_set.add(str(row[col]))

        # --- Per-year share check (flag if near-zero in any single year) ---
        if has_year:
            df_mkt_y = (
                df_mkt.with_columns(pl.col(rsd_col).dt.year().alias("_year"))
                .filter(pl.col("_year").is_not_null())
            )
            for year in df_mkt_y.get_column("_year").drop_nulls().unique().to_list():
                df_y = df_mkt_y.filter(pl.col("_year") == year)
                total_y = len(df_y)
                if total_y == 0:
                    continue
                for col, nz_set in [("BU", nz_bu), ("Country", nz_country)]:
                    if col not in df_y.columns:
                        continue
                    vc_y = (
                        df_y.select(pl.col(col).cast(pl.Utf8).str.strip_chars().fill_null("").alias(col))
                        .group_by(col).agg(pl.len().alias("cnt"))
                    )
                    for row in vc_y.iter_rows(named=True):
                        # Add to removal set if below threshold in this specific year
                        if round(row["cnt"] / total_y * 100, 1) <= pct:
                            nz_set.add(str(row[col]))

        # Record results and queue removal conditions for the deferred filter step
        near_zero_removed[mkt_label] = {"BU": sorted(nz_bu), "Country": sorted(nz_country)}
        if nz_bu:
            remove_conditions.append((mkt_val, "BU", frozenset(nz_bu)))
        if nz_country:
            remove_conditions.append((mkt_val, "Country", frozenset(nz_country)))

    # Apply all removal conditions in a single pass per condition to minimise re-scans
    for mkt_val, col, vals in remove_conditions:
        mkt_expr = pl.col("Market").cast(pl.Utf8).str.strip_chars().str.to_uppercase().eq(mkt_val)
        df = df.filter(~(mkt_expr & pl.col(col).cast(pl.Utf8).str.strip_chars().is_in(vals)))

    # Persist audit log for downstream verification / debugging
    os.makedirs(out_dir, exist_ok=True)
    nz_path = os.path.join(out_dir, "near_zero_removed.json")
    with open(nz_path, "w", encoding="utf-8") as f:
        json.dump(near_zero_removed, f, indent=2, ensure_ascii=False)

    pct_display = near_zero_pct if isinstance(near_zero_pct, dict) else {"Americas": near_zero_pct, "EMEA": near_zero_pct}
    print(f"  Thresholds (pct): {pct_display}")
    print(f"  Rows: {before:,} -> {len(df):,}  (removed {before - len(df):,})")
    print(f"  List saved to: {nz_path}")
    for mkt_label in ["Americas", "EMEA"]:
        info = near_zero_removed.get(mkt_label, {"BU": [], "Country": []})
        bu_list = info.get("BU", [])
        co_list = info.get("Country", [])
        print(f"  {mkt_label}: BU removed ({len(bu_list)}): {bu_list[:10]}{'...' if len(bu_list) > 10 else ''}")
        print(f"           Country removed ({len(co_list)}): {co_list[:10]}{'...' if len(co_list) > 10 else ''}")
    return df


def step_remove_incomplete_years(
    df: pl.DataFrame,
    out_dir: str = os.path.join("data", "demand"),
    remove: bool = True,
) -> pl.DataFrame:
    """Step 13 – Remove BU/Country not present in all years per market.

    Year-completeness check: demand forecasting models require consistent time-series
    data.  A BU or Country that exists in some years but not others introduces gaps
    that make year-over-year comparisons unreliable and can cause division-by-zero
    or missing-bar issues in charts.

    For each market (Americas, EMEA):
    1. Determine the full set of years present in that market's data.
    2. For each unique BU value, check which years it appears in.
    3. If any year from the full set is missing, the BU is flagged for removal.
       The specific missing years are recorded in the audit log to aid diagnosis.
    4. The same logic is applied independently to the Country dimension.

    This check is deliberately strict: even one missing year causes exclusion.
    The intent is that the downstream demand model always sees a balanced panel.
    If only one year is present in the dataset (len(all_years) < 2), the check is
    skipped because year-completeness is undefined for a single-year slice.

    As with step 12, removal is deferred: all violators are collected first and
    the filter is applied afterwards to avoid modifying the frame during iteration.
    Results are saved to special_cases_removed.json with per-value reason strings
    that identify which years were missing, supporting manual review.
    """
    print("\n" + "=" * 60)
    print("[STEP 13] Incomplete years BU/Country removal")

    rsd_col = "Requirement Start Date"
    if not remove or "Market" not in df.columns or rsd_col not in df.columns:
        print("  Skipping (remove=False or required columns missing).")
        return df

    special_cases_removed = {}  # audit record: market → {BU: {val: reason}, Country: {val: reason}}
    before = len(df)
    remove_conditions = []  # deferred: [(mkt_val, col, frozenset_of_values), ...]

    for mkt_label, mkt_val in [("Americas", "AMERICAS"), ("EMEA", "EMEA")]:
        mkt_expr = pl.col("Market").cast(pl.Utf8).str.strip_chars().str.to_uppercase().eq(mkt_val)
        # Extract the year from RSD and attach as a helper column for grouping
        df_mkt = (
            df.filter(mkt_expr)
            .with_columns(pl.col(rsd_col).dt.year().cast(pl.Int32).alias("_year"))
            .filter(pl.col("_year").is_not_null())
        )
        if len(df_mkt) == 0:
            special_cases_removed[mkt_label] = {"BU": {}, "Country": {}}
            continue

        # all_years is the reference set — every BU/Country must appear in ALL of these
        all_years = sorted(df_mkt.get_column("_year").drop_nulls().unique().to_list())
        if len(all_years) < 2:
            # Cannot define "completeness" when only one year exists; skip this market
            special_cases_removed[mkt_label] = {"BU": {}, "Country": {}}
            continue

        bu_reasons: dict = {}
        country_reasons: dict = {}

        for col, reasons in [("BU", bu_reasons), ("Country", country_reasons)]:
            if col not in df_mkt.columns:
                continue
            # Get every unique value in this dimension (e.g. every distinct BU name)
            vals = (
                df_mkt.select(pl.col(col).cast(pl.Utf8).str.strip_chars().fill_null("").alias(col))
                .unique().get_column(col).to_list()
            )
            for val in vals:
                if val == "":
                    continue  # skip blank/null placeholders
                # Find which years this specific value actually appears in
                years_present = (
                    df_mkt.filter(pl.col(col).cast(pl.Utf8).str.strip_chars().eq(val))
                    .get_column("_year").drop_nulls().unique().to_list()
                )
                missing = [y for y in all_years if y not in years_present]
                if missing:
                    # Record human-readable reason for the audit log
                    reasons[val] = "not present in all years; missing years: " + ", ".join(map(str, sorted(missing)))

        special_cases_removed[mkt_label] = {"BU": bu_reasons, "Country": country_reasons}
        if bu_reasons:
            remove_conditions.append((mkt_val, "BU", frozenset(bu_reasons.keys())))
        if country_reasons:
            remove_conditions.append((mkt_val, "Country", frozenset(country_reasons.keys())))

    # Apply all queued removal conditions in a single pass per condition
    for mkt_val, col, vals in remove_conditions:
        mkt_expr = pl.col("Market").cast(pl.Utf8).str.strip_chars().str.to_uppercase().eq(mkt_val)
        df = df.filter(~(mkt_expr & pl.col(col).cast(pl.Utf8).str.strip_chars().is_in(vals)))

    # Persist the audit log with reason strings for each removed value
    os.makedirs(out_dir, exist_ok=True)
    special_path = os.path.join(out_dir, "special_cases_removed.json")
    with open(special_path, "w", encoding="utf-8") as f:
        json.dump(special_cases_removed, f, indent=2, ensure_ascii=False)

    print(f"  Rows: {before:,} -> {len(df):,}  (removed {before - len(df):,})")
    print(f"  Reasons saved to: {special_path}")
    for mkt_label in ["Americas", "EMEA"]:
        info = special_cases_removed.get(mkt_label, {"BU": {}, "Country": {}})
        for dim in ["BU", "Country"]:
            d = info.get(dim, {})
            if not d:
                continue
            print(f"  {mkt_label} {dim} removed ({len(d)}):")
            for name, reason in list(d.items())[:5]:
                print(f"    - {name}: {reason}")
            if len(d) > 5:
                print(f"    ... and {len(d) - 5} more (see {special_path})")
    return df


def step_export_demand_json(
    df: pl.DataFrame,
    out_dir: str = os.path.join("data", "demand"),
    corrected_path: str = "",
) -> pl.DataFrame:
    """Step 14 – Export demand JSONs per market + year, save corrected CSV.

    This is the final output step of the pipeline.  It produces two categories
    of demand summary files plus the corrected CSV:

    1. Overall demand JSONs (one per market):
       - Americas_demand.json
       - EMEA_demand.json
       Each file contains total_demand (row count) and breakdown dicts for
       SO GRADE, BU, and Country, with counts and percentage shares.

    2. Year-wise demand JSONs (one per market per year):
       - Americas_demand_2023.json, Americas_demand_2024.json, etc.
       - EMEA_demand_2023.json, EMEA_demand_2024.json, etc.
       These allow the frontend / downstream model to render year-on-year trends
       without re-aggregating the full CSV.

    3. Corrected CSV (DFC_YTD_..._V2_corrected.csv):
       The final cleaned and corrected DataFrame saved for archival and as a
       reproducibility artefact; this is the "source of truth" after all pipeline
       transformations.

    The nested helper _build_demand() centralises the aggregation logic:
    it groups by each dimension column, counts rows, sorts descending, and
    formats each value as "N,NNN (X.X%)" for direct consumption by charts.
    """
    print("\n" + "=" * 60)
    print("[STEP 14] Demand JSON export + save corrected CSV")

    rsd_col = "Requirement Start Date"

    def _build_demand(sub: pl.DataFrame) -> dict:
        """Compute demand summary dict for a market/year slice.

        Returns a dict with:
        - total_demand: integer row count
        - SO GRADE, BU, Country: dicts mapping value → "count (pct%)" string,
          sorted by count descending so the largest categories appear first.
        """
        total = len(sub)
        if total == 0:
            return {"total_demand": 0, "SO GRADE": {}, "BU": {}, "Country": {}}
        result: dict = {"total_demand": total}
        for col in ["SO GRADE", "BU", "Country"]:
            if col not in sub.columns:
                result[col] = {}
                continue
            # group_by + agg(pl.len()) is the polars equivalent of pandas value_counts()
            vc = (
                sub.select(pl.col(col).cast(pl.Utf8).str.strip_chars().fill_null("").alias(col))
                .group_by(col).agg(pl.len().alias("cnt"))
                .sort("cnt", descending=True)  # highest-demand categories first
            )
            # Format each value as "N,NNN (X.X%)" for direct use in UI charts
            result[col] = {
                row[col]: f"{row['cnt']:,} ({row['cnt'] / total * 100:.1f}%)"
                for row in vc.iter_rows(named=True)
            }
        return result

    os.makedirs(out_dir, exist_ok=True)
    # Reusable market normalisation expression (uppercase, strip whitespace)
    market_upper = pl.col("Market").cast(pl.Utf8).str.strip_chars().str.to_uppercase()

    # --- Overall demand summaries (across all years) ---
    df_americas = df.filter(market_upper.eq("AMERICAS"))
    df_emea = df.filter(market_upper.eq("EMEA"))

    americas_demand = _build_demand(df_americas)
    emea_demand = _build_demand(df_emea)

    # Write the overall per-market JSON files
    for fname, obj in [
        ("Americas_demand.json", americas_demand),
        ("EMEA_demand.json",    emea_demand),
    ]:
        with open(os.path.join(out_dir, fname), "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2, ensure_ascii=False)

    # --- Year-wise demand summaries ---
    year_files = []
    if rsd_col in df.columns:
        # Attach integer year helper column for partitioning
        df_y = (
            df.with_columns(pl.col(rsd_col).dt.year().cast(pl.Int32).alias("_year"))
            .filter(pl.col("_year").is_not_null())
        )
        years = sorted(df_y.get_column("_year").drop_nulls().unique().to_list())
        for mkt_name, mkt_val in [("Americas", "AMERICAS"), ("EMEA", "EMEA")]:
            df_sub = df_y.filter(market_upper.eq(mkt_val))
            for yr in years:
                # Slice to this market + year and compute the demand summary
                df_yr = df_sub.filter(pl.col("_year") == yr)
                fname = f"{mkt_name}_demand_{yr}.json"
                with open(os.path.join(out_dir, fname), "w", encoding="utf-8") as f:
                    json.dump(_build_demand(df_yr), f, indent=2, ensure_ascii=False)
                year_files.append(fname)

    print(f"  Dir: {out_dir}")
    print(f"  Americas: Americas_demand.json (total_demand={americas_demand['total_demand']:,})")
    print(f"  EMEA:     EMEA_demand.json (total_demand={emea_demand['total_demand']:,})")
    if year_files:
        print(f"  Year-wise: {', '.join(year_files)}")

    # Save the fully corrected DataFrame as a CSV artefact for reproducibility
    if corrected_path:
        df.write_csv(corrected_path)
        print(f"  Saved corrected CSV -> {corrected_path}")

    return df


def step_drop_nulls(df: pl.DataFrame, out_path: str) -> pl.DataFrame:
    """Step 8 – Report nulls, drop rows with any null → DFC_YTD_..._V2.csv.

    After the date-parsing and filter steps, any remaining null values in the
    dataset indicate either:
    - Date columns that could not be parsed by any of the format strings in step 5
      (the column stays null for those rows).
    - Columns that were populated in one source file but missing in another
      (e.g. a new field introduced in the 2025 extract).

    Rows with ANY null value are dropped here because:
    1. The downstream demand aggregations (group_by / percentage calculations) require
       complete rows to produce accurate counts.
    2. Rows with null dates cannot be used for year-based partitioning in steps 12-14.

    A pre-drop null audit is printed to make it easy to spot which columns are
    introducing nulls — useful for diagnosing changes in the upstream data format.

    polars null_count() returns a 1-row summary DataFrame; .row(0) extracts it as
    a plain tuple aligned with df.columns for zipping.
    """
    print("\n" + "=" * 60)
    print("[STEP 8] Null analysis & drop")

    total = df.height
    # null_count() is a single-pass aggregate; .row(0) gives the count tuple
    null_counts = df.null_count().row(0)
    any_nulls = False
    # Print a per-column null summary before dropping so the operator can see
    # which fields are contributing to row loss
    for col, cnt in zip(df.columns, null_counts):
        if cnt > 0:
            print(f"  {col}: {cnt}/{total} rows have nulls")
            any_nulls = True
    if not any_nulls:
        print("  No nulls found.")

    before = len(df)
    # Drop rows where ANY column contains a null (all-column null check)
    df = df.drop_nulls()
    print(f"  Shape before drop: {before:,} rows | After: {len(df):,} rows")

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    df.write_csv(out_path)
    print(f"  Saved -> {out_path}")
    return df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="DFC preprocessing pipeline → cleaned_data3.csv",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Input files
    p.add_argument(
        "--ytd-files",
        nargs="+",
        default=[
            os.path.join("data", "DFC_YTD_2023-utf.csv"),
            os.path.join("data", "DFC_YTD_2024-utf.csv"),
            os.path.join("data", "DFC_YTD_2025.csv"),
        ],
        metavar="PATH",
        help="One or more YTD CSV file paths (space-separated). "
             "Default: data/DFC_YTD_20{23,24,25}.csv",
    )
    p.add_argument(
        "--mtd-file",
        default=os.path.join("data", "DFC_MTD.csv"),
        metavar="PATH",
        help="MTD (open-demand) CSV file path. Default: data/DFC_MTD_2025.csv",
    )

    # Output
    p.add_argument(
        "--output-dir",
        default="data",
        metavar="DIR",
        help="Directory for output CSVs (cleaned_data1/2/3.csv). Default: data/",
    )

    # Column selection
    p.add_argument(
        "--cols-to-keep",
        default=None,
        metavar="JSON",
        help="JSON array of columns to SELECT when reading source CSVs. "
             "Can be a literal JSON string or a path to a .json file. "
             "Defaults to the notebook's columns_imp list.",
    )
    p.add_argument(
        "--cols-to-remove",
        default=None,
        metavar="JSON",
        help="JSON array of columns to DROP after loading (step 2). "
             "Can be a literal JSON string or a path to a .json file. "
             "Defaults to the notebook's columns_to_remove list.",
    )

    # Business filters
    p.add_argument(
        "--practice-area",
        default=None,
        metavar="PA",
        help="Full Practice Area name to process (e.g. 'EPS', 'ADM', 'Digital Engineering'). "
             "Overrides the 'Practice Area' key in --filters / DEFAULT_FILTERS while keeping "
             "all other filter values. Shorthand for running a single PA without editing --filters.",
    )
    p.add_argument(
        "--filters",
        default=None,
        metavar="JSON",
        help='JSON object mapping column names -> list of allowed values (step 3). '
             'Example: \'{"SO TYPE":["STA"],"Market":["Americas"]}\'. '
             "Can also be a path to a .json file. "
             "Defaults to the notebook's FILTERS dict.",
    )

    # Grade settings
    p.add_argument(
        "--grades-col",
        default="SO GRADE",
        metavar="COL",
        help="Name of the grade column. Default: 'SO GRADE'",
    )
    p.add_argument(
        "--grades-to-combine",
        default=None,
        metavar="JSON",
        help='JSON object where each key is the NEW label and the value is a list '
             'of existing grades to merge into it. '
             'Example: \'{"GenC":["PT","PAT","PA","P"]}\'. '
             "Can also be a path to a .json file. "
             "Defaults to {\"GenC\":[\"PT\",\"PAT\",\"PA\",\"P\"]}.",
    )
    p.add_argument(
        "--grades-to-remove",
        default=None,
        metavar="JSON",
        help="JSON array of grade values to EXCLUDE entirely (case-insensitive). "
             'Example: \'["cont","C"]\'. '
             "Can also be a path to a .json file. "
             'Defaults to ["cont"].',
    )

    # RSD year guardrail
    p.add_argument(
        "--rsd-min-year",
        type=int,
        default=2023,
        metavar="YEAR",
        help="Minimum Requirement Start Date year allowed (step 6). Default: 2023",
    )

    # BU / PA correction (steps 9-14)
    p.add_argument(
        "--sbu-map",
        default=os.path.join("data", "SBU-BU Mapping.xlsx"),
        metavar="PATH",
        help="Path to the SBU-BU mapping Excel file used in step 9. "
             "Default: data/SBU-BU Mapping.xlsx",
    )
    p.add_argument(
        "--no-near-zero-removal",
        action="store_true",
        default=False,
        help="Disable step 12 (near-zero BU/Country removal).",
    )
    p.add_argument(
        "--no-incomplete-years-removal",
        action="store_true",
        default=False,
        help="Disable step 13 (incomplete-years BU/Country removal).",
    )

    return p.parse_args()


def main():
    """Orchestrate the full 14-step DFC preprocessing pipeline.

    Resolves CLI args, routes outputs to a PA-specific subdirectory, runs each
    pipeline step in order, and writes a machine-readable audit JSON that captures
    row counts before/after every step for reproducibility and debugging.
    """
    args = parse_args()

    # ── Resolve JSON / default args ──────────────────────────────────────────
    # Each CLI arg can be provided as a JSON literal, a path to a JSON file, or
    # omitted (in which case the corresponding DEFAULT_* constant is used).
    args.cols_to_keep = (
        load_json_arg(args.cols_to_keep, "cols-to-keep")
        if args.cols_to_keep
        else DEFAULT_COLS_TO_KEEP
    )
    args.cols_to_remove = (
        load_json_arg(args.cols_to_remove, "cols-to-remove")
        if args.cols_to_remove
        else DEFAULT_COLS_TO_REMOVE
    )
    args.filters = (
        load_json_arg(args.filters, "filters")
        if args.filters
        else DEFAULT_FILTERS
    )
    args.grades_to_combine = (
        load_json_arg(args.grades_to_combine, "grades-to-combine")
        if args.grades_to_combine
        else DEFAULT_GRADES_TO_COMBINE
    )
    args.grades_to_remove = (
        load_json_arg(args.grades_to_remove, "grades-to-remove")
        if args.grades_to_remove
        else DEFAULT_GRADES_TO_REMOVE
    )

    # If --practice-area was given, override just the Practice Area filter key.
    # This is a convenience shorthand so callers don't need to pass the full
    # --filters JSON just to change one PA.
    if args.practice_area:
        args.filters["Practice Area"] = [args.practice_area.strip()]

    # Derive PA abbreviation from the Practice Area filter and route outputs to
    # data/{abbrev}/ (e.g. data/EPS/ or data/DE/).
    # All intermediate and final CSVs for this run land in this subdirectory.
    practice_areas = args.filters.get("Practice Area", [])
    if practice_areas:
        abbrev = "_".join(pa_abbrev(pa) for pa in practice_areas)
    else:
        abbrev = "ALL"
    out = os.path.join(args.output_dir, abbrev)
    os.makedirs(out, exist_ok=True)
    print(f"\n[INFO]  Practice Area abbreviation: {abbrev!r} -> output dir: {out}")

    # ── DE flag: steps 9-11 (BU/EMEA corrections) are DE-only ──────────────
    # BU correction (SBU mapping), GGM conversion, and EMEA BU renaming are
    # specific to the Digital Engineering (DE) practice area.  For other PAs
    # these steps are skipped.  GGM is also stripped from the Market filter for
    # non-DE runs because GGM data without step 10 would leave unresolved rows.
    is_de = (abbrev == "DE")
    if not is_de and "GGM" in args.filters.get("Market", []):
        args.filters["Market"] = [m for m in args.filters["Market"] if m != "GGM"]
        print(f"[INFO]  Non-DE practice area – 'GGM' removed from Market filter automatically")

    # ── Pipeline audit tracker ───────────────────────────────────────────────
    # _rec() is called after every step to record before/after row counts.
    # The full audit list is serialised to pipeline_audit.json at the end.
    audit: list = []

    def _rec(step_num: int, name: str, rows_before: int, rows_after: int) -> None:
        """Append a step audit record to the audit list."""
        audit.append({
            "step": step_num,
            "name": name,
            "rows_before": rows_before,
            "rows_after": rows_after,
            "rows_removed": rows_before - rows_after,
        })

    # ── Run pipeline ─────────────────────────────────────────────────────────

    # ---- Step 1: Load and concatenate YTD + MTD source files ----------------
    # Reads only the whitelisted columns to reduce memory footprint; stacks
    # multiple YTD annual files and the MTD open-demand snapshot vertically.
    df = step_load(args)
    _rec(1, "Load source files", 0, len(df))

    # ---- Step 2: Drop fully-empty and specified columns → cleaned_data1.csv --
    # Removes auto-detected all-null columns plus the explicit drop list, then
    # writes the first intermediate checkpoint CSV.
    n = len(df)
    df = step_clean_columns(df, args.cols_to_remove, os.path.join(out, "cleaned_data1.csv"))
    _rec(2, "Column cleaning", n, len(df))

    # ---- Step 3: Apply business filters → cleaned_data2.csv -----------------
    # Keeps only rows matching the allowed values for SO TYPE, Project Type,
    # Practice Area, Market, Billability Type, and Cancellation Reason.
    n = len(df)
    df = step_filter(df, args.filters, os.path.join(out, "cleaned_data2.csv"))
    _rec(3, "Business filters", n, len(df))

    # ---- Step 4: Exclude and combine SO GRADE values ------------------------
    # Drops grades like "cont", "VP", "TD Trainee" (noise / out-of-scope), then
    # merges granular entry-level grades (PT, PAT, PA, P) into a single "GenC" label.
    n = len(df)
    df = step_combine_grades(df, args.grades_col, args.grades_to_combine, args.grades_to_remove)
    _rec(4, "Grade exclusion & combining", n, len(df))

    # ---- Step 5: Parse date-like columns to pl.Date -------------------------
    # Tries multiple date format strings in cascade to handle mixed-format
    # date values produced by different regional DFC exports.
    n = len(df)
    df = step_clean_dates(df)
    _rec(5, "Date standardisation", n, len(df))

    # ---- Step 6: Remove rows where RSD year is outside [min_year, 2025] -----
    # Guards against stale or future-dated SOs that would distort year-wise
    # demand aggregations in steps 12-14.
    n = len(df)
    df = step_filter_rsd_year(df, min_year=args.rsd_min_year)
    _rec(6, f"RSD year guardrail ({args.rsd_min_year}-2026)", n, len(df))

    # ---- Step 7: Remove rows where RSD < SO Submission Date -----------------
    # Temporal consistency check: a requirement cannot start before it was raised.
    # Such rows indicate data-entry errors or date-parsing artefacts.
    n = len(df)
    df = step_filter_rsd_before_ssd(df)
    _rec(7, "RSD >= SSD guardrail", n, len(df))

    # ---- Step 8: Drop rows with any null → DFC_YTD_..._V2.csv --------------
    # Rows that still have null values (typically from unparseable date strings
    # in step 5) are removed.  Writes the V2 checkpoint CSV.
    final_filename = build_final_filename(args.ytd_files, args.filters)
    n = len(df)
    df = step_drop_nulls(df, os.path.join(out, final_filename))
    _rec(8, "Null drop", n, len(df))

    # ── Steps 9-11: DE-only BU/EMEA corrections ──────────────────────────────
    # These three steps apply BU normalisation logic that is specific to the
    # Digital Engineering practice area.  They are skipped for all other PAs.
    if is_de:
        # ---- Step 9: Correct BU from SBU-BU mapping (Americas only) ---------
        # Left-joins the SBU-BU mapping Excel to overwrite stale BU values for
        # Americas rows using the authoritative SBU1 → BU lookup.
        n = len(df)
        df = step_correct_bu(df, map_path=args.sbu_map)
        _rec(9, "BU correction from SBU mapping (DE/Americas only)", n, len(df))

        # ---- Step 10: Convert GGM rows to EMEA/APJ, then drop APJ -----------
        # Reclassifies GGM umbrella-market rows into their leaf markets using the
        # Market Unit column, then removes all APJ rows (out of scope).
        n = len(df)
        df = step_convert_ggm_drop_apj(df)
        _rec(10, "GGM Market conversion + APJ drop (DE only)", n, len(df))

        # ---- Step 11: EMEA BU corrections (rename/merge/split) --------------
        # Applies five bespoke EMEA BU corrections: drops Mobility, renames
        # Transport-UK → T&H-UK, merges UK BUs → RCGT&H-UK, renames South
        # Europe → SPAI, and splits Benelux into Belux / Netherlands by SBU1.
        n = len(df)
        df = step_correct_emea_bu(df)
        _rec(11, "EMEA BU corrections (DE only)", n, len(df))
    else:
        # Non-DE PAs skip steps 9-11; audit records a no-op for those step numbers
        print(f"\n[INFO]  Skipping steps 9-11 (BU/EMEA corrections) – DE only, current PA: {abbrev}")

    demand_dir = os.path.join("data", "demand")

    # ---- Step 12: Remove near-zero BU/Country per market --------------------
    # Flags any BU or Country whose row share falls at or below the threshold
    # (0.0% for Americas, 0.1% for EMEA) either overall or in any individual year,
    # then removes all matching rows and saves the removal list to JSON.
    n = len(df)
    df = step_remove_near_zero(
        df,
        near_zero_pct={"Americas": 0.0, "EMEA": 0.1},
        out_dir=demand_dir,
        remove=not args.no_near_zero_removal,
    )
    _rec(12, "Near-zero BU/Country removal", n, len(df))

    # ---- Step 13: Remove BU/Country not present in all years ----------------
    # Ensures that every BU and Country in the final dataset appears in every
    # year present in that market, producing a balanced panel for trend analysis.
    # The list of removed values with per-value reasons is saved to JSON.
    n = len(df)
    df = step_remove_incomplete_years(
        df,
        out_dir=demand_dir,
        remove=not args.no_incomplete_years_removal,
    )
    _rec(13, "Incomplete years BU/Country removal", n, len(df))

    # ---- Step 14: Export demand JSONs + save corrected CSV ------------------
    # Writes overall and year-wise demand summary JSONs for Americas and EMEA,
    # and saves the final corrected DataFrame as DFC_YTD_..._V2_corrected.csv.
    corrected_filename = final_filename.replace("_V2.csv", "_V2_corrected.csv")
    corrected_path = os.path.join(out, corrected_filename)

    n = len(df)
    df = step_export_demand_json(df, out_dir=demand_dir, corrected_path=corrected_path)
    _rec(14, "Demand JSON export + save corrected CSV", n, len(df))

    # ── Pipeline audit JSON ───────────────────────────────────────────────────
    # Assemble and write a comprehensive audit object that records all pipeline
    # parameters and the before/after row counts for every step.  This file is
    # the primary artefact for debugging runs and comparing results across dates.
    market_vals = args.filters.get("Market", [])
    market_str = ", ".join(market_vals) if market_vals else "ALL"
    initial_rows = audit[0]["rows_after"] if audit else 0

    # Final market-level row counts for the summary section of the audit
    _mkt_upper = pl.col("Market").cast(pl.Utf8).str.strip_chars().str.to_uppercase()
    americas_final = df.filter(_mkt_upper.eq("AMERICAS")).height
    emea_final     = df.filter(_mkt_upper.eq("EMEA")).height

    audit_obj = {
        "practice_area": abbrev,
        "de_corrections_applied": is_de,
        "filters_applied": {
            k: v for k, v in args.filters.items()
        },
        "grades_excluded": args.grades_to_remove,
        "grades_combined": args.grades_to_combine,
        "rsd_year_range": f"{args.rsd_min_year}-2025",
        "near_zero_thresholds_pct": {"Americas": 0.0, "EMEA": 0.1},
        "pipeline": audit,             # full step-by-step row-count trace
        "summary": {
            "initial_rows": initial_rows,
            "final_rows": len(df),
            "total_removed": initial_rows - len(df),
            "market_breakdown": {
                "Americas": americas_final,
                "EMEA": emea_final,
                "other": len(df) - americas_final - emea_final,
            },
        },
    }
    audit_path = os.path.join(out, "pipeline_audit.json")
    with open(audit_path, "w", encoding="utf-8") as f:
        json.dump(audit_obj, f, indent=2, ensure_ascii=False)
    print(f"\n[AUDIT] Pipeline audit saved -> {audit_path}")

    # ── Final summary ────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"[DONE]  Final shape: {df.shape[0]:,} rows x {df.shape[1]} columns")
    print(f"        Output file: {os.path.join(os.path.abspath(out), corrected_filename)}")
    print("=" * 60)
    print(f"\n[FINAL] Total DFC rows: {df.shape[0]:,} | Market: {market_str}")


if __name__ == "__main__":
    main()

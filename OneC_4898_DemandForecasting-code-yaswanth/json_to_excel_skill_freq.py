#!/usr/bin/env python3
"""
Convert skill_normalization_llm2.json to Excel with frequency counts from SBU data.

Usage:
    python json_to_excel_skill_freq.py          # defaults to DE
    python json_to_excel_skill_freq.py EPS
    python json_to_excel_skill_freq.py ADM

- Reads skills/skill_normalization_llm2.json (canonical skill -> variants list)
- Counts how often each canonical skill appears in the SBU corrected CSV
  (via matching any of its variants against the 'Technical Skills Required' column)
- Outputs an Excel file sorted by frequency (highest -> lowest)
  saved in the same folder as the JSON file.
"""

import argparse
import json
from pathlib import Path

import pandas as pd

# ── SBU -> CSV path mapping ────────────────────────────────────────────────────
SBU_CSV_MAP: dict[str, Path] = {
    "DE":  Path("data/DE/DFC_YTD_2023-2025_DE_V2_corrected.csv"),
    "EPS": Path("data/EPS/DFC_YTD_2023-2025_EPS_V2_corrected.csv"),
    "ADM": Path("data/ADM/DFC_YTD_2023-2025_ADM_V2_corrected.csv"),
}

# ── Args ───────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Skill frequency Excel export by SBU")
parser.add_argument(
    "sbu",
    nargs="?",
    default="DE",
    choices=list(SBU_CSV_MAP),
    help=f"SBU to process (default: DE). Options: {', '.join(SBU_CSV_MAP)}",
)
args = parser.parse_args()
SBU = args.sbu.upper()

# ── Paths ─────────────────────────────────────────────────────────────────────
JSON_PATH   = Path("skills/skill_normalization_llm2.json")
CSV_PATH    = SBU_CSV_MAP[SBU]
OUTPUT_PATH = JSON_PATH.with_name(f"skill_normalization_{SBU}.xlsx")
FREQ_COL    = f"Frequency ({SBU})"
SKILL_COL   = "Technical Skills Required"

print(f"SBU: {SBU}")

# ── Load ──────────────────────────────────────────────────────────────────────
print(f"Loading JSON: {JSON_PATH}")
with open(JSON_PATH, encoding="utf-8") as f:
    skill_map: dict[str, dict] = json.load(f)

print(f"Loading CSV:  {CSV_PATH}")
df = pd.read_csv(CSV_PATH, low_memory=False)

if SKILL_COL not in df.columns:
    raise ValueError(f"Column '{SKILL_COL}' not found in CSV. Available: {list(df.columns)}")

# ── Build variant -> canonical lookup (case-insensitive) ──────────────────────
variant_to_canonical: dict[str, str] = {}
for canonical, meta in skill_map.items():
    variants = meta.get("variants", []) if isinstance(meta, dict) else meta
    for v in variants:
        variant_to_canonical[v.strip().lower()] = canonical
    # also map the canonical itself
    variant_to_canonical[canonical.strip().lower()] = canonical

# ── Count frequency ───────────────────────────────────────────────────────────
freq: dict[str, int] = {canonical: 0 for canonical in skill_map}

for raw_cell in df[SKILL_COL].dropna():
    # skills are comma-separated inside each cell
    for raw_skill in str(raw_cell).split(","):
        token = raw_skill.strip().lower()
        if token and token in variant_to_canonical:
            canonical = variant_to_canonical[token]
            freq[canonical] = freq.get(canonical, 0) + 1

# ── Build output DataFrame ────────────────────────────────────────────────────
rows = []
for canonical, meta in skill_map.items():
    variants = meta.get("variants", []) if isinstance(meta, dict) else meta
    rows.append({
        "Canonical Skill": canonical,
        "Variants": ", ".join(variants),
        "Variant Count": len(variants),
        FREQ_COL: freq.get(canonical, 0),
    })

result_df = (
    pd.DataFrame(rows)
    .sort_values(FREQ_COL, ascending=False)
    .reset_index(drop=True)
)

# Add rank column
result_df.insert(0, "Rank", range(1, len(result_df) + 1))

# ── Export to Excel ───────────────────────────────────────────────────────────
print(f"Writing Excel: {OUTPUT_PATH}")
with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
    result_df.to_excel(writer, index=False, sheet_name="Skill Frequency")

    ws = writer.sheets["Skill Frequency"]

    # Auto-fit column widths (cap at 80)
    for col_cells in ws.columns:
        max_len = max((len(str(c.value or "")) for c in col_cells), default=10)
        ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 2, 80)

    # Freeze header row
    ws.freeze_panes = "A2"

print(f"\nDone! {len(result_df)} skills written.")
print(f"Top 10 by frequency:")
print(result_df[["Rank", "Canonical Skill", FREQ_COL]].head(10).to_string(index=False))

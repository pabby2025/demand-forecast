#!/usr/bin/env bash
# Full clustering pipeline per Practice Area: normalize -> demand clustering -> [pause] -> apply clusters.
# --unmapped: run ONLY the unmapped pass (no skill_normalized, no main clustering). Use after you already have
#   v2_unmapped_<Market>.csv from a previous full run.

set -euo pipefail

# ==============================================================================
# Practice Areas to process (space-separated abbreviations).
# Add or remove PAs here. The full pipeline (or unmapped pass) is run for each.
# ==============================================================================
PRACTICE_AREAS="DE"

# ==============================================================================
# Per-PA config.
# Keys = PA abbreviation. Edit values as needed.
# EXCLUDE_SKILLS  : stripped from row data and clusters (not used in Jaccard).
# PRIORITY_SKILLS : force-assign rows to the cluster containing this skill (mapped mode).
# PRIMARY_SKILLS  : second-pass primary skills for unmapped rows (Jaccard on these only).
# ==============================================================================
declare -A PA_EXCLUDE_SKILLS
declare -A PA_PRIORITY_SKILLS
declare -A PA_PRIMARY_SKILLS

PA_EXCLUDE_SKILLS["DE"]="API Development"
PA_PRIORITY_SKILLS["DE"]="SharePoint,iOS,Android,React Native"
PA_PRIMARY_SKILLS["DE"]="Java,Spring Boot,JavaScript,React,Angular,.NET,C#,AWS,Azure,SQL,Python,TypeScript,Microservices,Kafka,DevOps"

PA_EXCLUDE_SKILLS["EPS"]="API Development"
PA_PRIORITY_SKILLS["EPS"]="SAP,Pega"
PA_PRIMARY_SKILLS["EPS"]="SAP,ABAP,Oracle,Pega,Java,Spring Boot,SQL,Azure,AWS,.NET,Python,Microservices,JavaScript,ServiceNow"

PA_EXCLUDE_SKILLS["ADM"]="API Development"
PA_PRIORITY_SKILLS["ADM"]="COBOL,ServiceNow"
PA_PRIMARY_SKILLS["ADM"]="Java,Spring Boot,.NET,COBOL,Oracle,SQL,DB2,ServiceNow,Azure,AWS,Python,Microservices,Shell Scripting/Linux,Angular,Node JS,JavaScript"

# ==============================================================================
# Runtime defaults
# ==============================================================================
RUN_UNMAPPED_ONLY="false"
YEAR_RANGE="2023-2025"

usage() {
  cat <<EOF
Usage: $(basename "$0") [--unmapped] [--year-range YYYY-YYYY]

  (no flag)          Full pipeline for each PA: skill_normalized -> skill_clusters_demand -> pause -> apply_clusters.
  --unmapped         Unmapped pass ONLY (skip normalization and main clustering). Requires existing
                     data/{PA}/DFC_YTD_*_V2_unmapped_<Market>.csv from a previous full run.
  --year-range       Year range string used in CSV filenames (default: $YEAR_RANGE).

  Practice areas processed: $PRACTICE_AREAS  (edit PRACTICE_AREAS at the top of this file).
  Per-PA config (EXCLUDE_SKILLS, PRIORITY_SKILLS, PRIMARY_SKILLS) is set per PA above.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --unmapped)
      RUN_UNMAPPED_ONLY="true"
      shift
      ;;
    --year-range)
      if [[ $# -lt 2 ]]; then
        echo "Error: --year-range requires a value" >&2; usage; exit 1
      fi
      YEAR_RANGE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

echo "======================================================================"
echo "Clustering pipeline | RUN_UNMAPPED_ONLY=$RUN_UNMAPPED_ONLY | YEAR_RANGE=$YEAR_RANGE"
echo "Practice Areas: $PRACTICE_AREAS"
echo "======================================================================"

for PA in $PRACTICE_AREAS; do
  EXCLUDE_SKILLS="${PA_EXCLUDE_SKILLS[$PA]:-}"
  PRIORITY_SKILLS="${PA_PRIORITY_SKILLS[$PA]:-}"
  PRIMARY_SKILLS="${PA_PRIMARY_SKILLS[$PA]:-}"

  echo
  echo "======================================================================"
  echo "PA: $PA"
  echo "  EXCLUDE_SKILLS  = ${EXCLUDE_SKILLS:-<none>}"
  echo "  PRIORITY_SKILLS = ${PRIORITY_SKILLS:-<none>}"
  echo "======================================================================"

  if [[ "$RUN_UNMAPPED_ONLY" == "true" ]]; then
    echo
    echo "Unmapped pass only (skipping normalization and main clustering) ..."
    python skill_clusters_demand.py \
      --unmapped \
      --exclude-skills "$EXCLUDE_SKILLS" \
      --practice-area  "$PA" \
      --year-range     "$YEAR_RANGE"

    echo
    echo "----------------------------------------------------------------------"
    echo "Pause before apply_clusters (unmapped) for PA=$PA."
    echo "  Edit skills/<Market>_${PA}/unmapped_clusters/skill_clusters.json if needed, then type 'yes' to continue."
    echo "----------------------------------------------------------------------"
    while true; do
      read -r -p "Type 'yes' to continue: " resp
      if [[ "${resp:-}" == "yes" ]]; then break; fi
      echo "Expected 'yes'. Try again or Ctrl+C to exit."
    done

    echo
    echo "Applying unmapped clusters for PA=$PA ..."
    python apply_clusters.py \
      --unmapped \
      --exclude-skills "$EXCLUDE_SKILLS" \
      --practice-area  "$PA" \
      --year-range     "$YEAR_RANGE"
    echo "Unmapped pass done for PA=$PA. Check skills/<Market>_${PA}/unmapped_clusters/ and data/${PA}/*unmapped*.csv"

  else
    echo
    echo "Step 1/4: Running skill_normalized.py for PA=$PA ..."
    python skill_normalized.py \
      --exclude-skills "$EXCLUDE_SKILLS" \
      --practice-area  "$PA" \
      --year-range     "$YEAR_RANGE"

    echo
    echo "Step 2/4: Running skill_clusters_demand.py (main clusters) for PA=$PA ..."
    _CLUSTER_ARGS=(
      --exclude-skills "$EXCLUDE_SKILLS"
      --practice-area  "$PA"
      --year-range     "$YEAR_RANGE"
    )
    if [[ -n "${PRIORITY_SKILLS:-}" ]]; then
      _CLUSTER_ARGS+=(--priority-skills "$PRIORITY_SKILLS")
    fi
    python skill_clusters_demand.py "${_CLUSTER_ARGS[@]}"

    echo
    echo "----------------------------------------------------------------------"
    echo "Step 3/4: Pause before apply_clusters for PA=$PA."
    echo "  Edit skills/<Market>_${PA}/skill_clusters.json if needed, then type 'yes' to continue."
    echo "----------------------------------------------------------------------"
    while true; do
      read -r -p "Type 'yes' to continue: " resp
      if [[ "${resp:-}" == "yes" ]]; then break; fi
      echo "Expected 'yes'. Try again or Ctrl+C to exit."
    done

    echo
    echo "Step 4/4: Running apply_clusters.py for PA=$PA ..."
    _APPLY_ARGS=(
      --priority-skills "$PRIORITY_SKILLS"
      --exclude-skills  "$EXCLUDE_SKILLS"
      --practice-area   "$PA"
      --year-range      "$YEAR_RANGE"
    )
    if [[ -n "${PRIMARY_SKILLS:-}" ]]; then
      _APPLY_ARGS+=(--primary-skills "$PRIMARY_SKILLS")
    fi
    python apply_clusters.py "${_APPLY_ARGS[@]}"
  fi

done

echo
echo "Clustering pipeline finished successfully for all Practice Areas: $PRACTICE_AREAS"

import os
# Pipeline configuration
SEED = 42
MIN_SUPPORT = 200
TOP_K = 25
SIM_THRESHOLD = 0.1
SIM_METRIC = "jaccard"  # options: 'jaccard','lift','ppmi'
COH_THRESHOLD = 0.5
TARGET_TOTAL = 200  # 200
MIN_OVERLAP = 2
MIN_JACCARD = 0.25  # 0.1
INPUT_CSV_PATH = "input/DFC_YTD_2023-2025 2.csv"
NORMALIZATION_MAP_PATH = "input/step1_skill_normalization_llm.json"
OUTPUT_MICROBUNDLE_DEF = "output/microbundle_definitions.json"
OUTPUT_DEMAND_MAP = "output/demand_to_microbundle_map.json"
OUTPUT_CLUSTERS = "output/kmeans_clusters.json"
OUTPUT_YTD_WITH_CLUSTERS = "output/DFC_YTD_2023-2025_with_microcluster.csv"
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:pass@localhost:5432/postgres")
OPTUNA_STUDY_NAME = "microcluster_hyperopt_v4"

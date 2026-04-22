import pandas as pd
import json
from config import INPUT_CSV_PATH, NORMALIZATION_MAP_PATH


def load_data():
    df = pd.read_csv(INPUT_CSV_PATH)
    df = df[["Unique ID", "Practice Area", "Technical Skills Required"]].dropna()
    df = df.drop(columns=["Practice Area", "Unique ID"])
    df["Technical Skills Required"] = df["Technical Skills Required"].apply(
        lambda x: [y.strip() for y in x.split(",")]
    )
    df.rename(columns={"Technical Skills Required": "tsr"}, inplace=True)
    with open(NORMALIZATION_MAP_PATH, "r", encoding="utf-8") as f:
        normalization_map = json.load(f)
    variant_to_canonical = {}
    for canonical, variants in normalization_map.items():
        variant_to_canonical[canonical] = canonical
        for variant in variants["variants"]:
            variant_to_canonical[variant] = canonical

    def normalize_skills(skills_list):
        normalized = []
        for skill in skills_list:
            if not skill:
                continue
            key = skill.strip()
            if key in normalized:
                continue
            if key in variant_to_canonical:
                canon = variant_to_canonical[key]
                if canon in normalized:
                    continue
                else:
                    normalized.append(canon)
            else:
                normalized.append(key)
        return normalized

    df["tsr"] = df["tsr"].apply(normalize_skills)
    return df

import numpy as np
import random
import json
import optuna
from config import (
    SEED,
    INPUT_CSV_PATH,
    OUTPUT_MICROBUNDLE_DEF,
    OUTPUT_DEMAND_MAP,
    OUTPUT_CLUSTERS,
    OUTPUT_YTD_WITH_CLUSTERS,
    DATABASE_URL,
    OPTUNA_STUDY_NAME,
)
from data_loader import load_data
from skill_graph import build_skill_graph
from embeddings import generate_embeddings
from clustering import cluster_skills
from microbundle import (
    generate_microbundles,
    select_final_microbundles,
    name_microbundles,
)
from mapping import (
    map_demands_to_microbundles,
    coverage_summary,
    write_demands_with_microcluster,
)

# Ensure consistent random seed is passed throughout the pipeline
if __name__ == "__main__":
    SEED = 42  # Fixed seed for reproducibility

    # Try to load best Optuna params if available
    best_params = None
    try:
        study = optuna.create_study(
            direction="maximize",
            study_name=OPTUNA_STUDY_NAME,
            storage=DATABASE_URL,
            load_if_exists=True,
        )
        best_params = study.best_params if study.best_trials else None
        print(f"Loaded best Optuna params: {best_params}")
    except Exception:
        best_params = None

    print("Loading and preprocessing data...")
    df = load_data()
    print("Building skill graph...")
    G, support = build_skill_graph(df)
    print("Generating embeddings...")
    emb_kwargs = {}
    if best_params:
        for k in ["dim", "walk_length", "window", "p", "q", "epochs"]:
            if k in best_params:
                emb_kwargs[k] = best_params[k]
    n2v, item_names, embeddings = generate_embeddings(G, seed=SEED, **emb_kwargs)
    print("Clustering skills...")
    clus_kwargs = {}
    if best_params:
        for k in ["min_k", "k_step", "batch_size", "n_init", "max_iter"]:
            if k in best_params:
                clus_kwargs[k] = best_params[k]
    clusters, representatives, skill_to_cluster, cluster_to_medoid = cluster_skills(
        embeddings, item_names, seed=SEED, **clus_kwargs
    )
    with open(OUTPUT_CLUSTERS, "w", encoding="utf-8") as f:
        json.dump(clusters, f, indent=4, ensure_ascii=False)
    print("Generating microbundles...")
    coh_threshold = None
    if best_params and "coh_threshold" in best_params:
        coh_threshold = best_params["coh_threshold"]
    try:
        if coh_threshold is not None:
            microbundles_by_cluster = generate_microbundles(clusters, G, n2v)
        else:
            microbundles_by_cluster = generate_microbundles(clusters, G, n2v)
    except TypeError:
        # For backward compatibility if generate_microbundles signature changes
        microbundles_by_cluster = generate_microbundles(clusters, G, n2v)
    print("Selecting final microbundles...")
    sel_kwargs = {}
    if best_params:
        for k in ["target_total", "min_overlap"]:
            if k in best_params:
                sel_kwargs[k] = best_params[k]
    final_microbundles = select_final_microbundles(
        microbundles_by_cluster, clusters, df, **sel_kwargs
    )
    print("Naming microbundles...")
    microbundle_list, microbundle_skill_sets = name_microbundles(final_microbundles)
    print(f"Total microbundles created: {len(microbundle_list)}")
    if len(microbundle_list) > 0:
        print("Sample microbundle:", microbundle_list[0])
    print("Mapping demands to microbundles...")
    mapped_demands = map_demands_to_microbundles(df, microbundle_skill_sets, G)
    print("Saving outputs...")
    with open(OUTPUT_MICROBUNDLE_DEF, "w", encoding="utf-8") as f:
        json.dump(microbundle_list, f, indent=4, ensure_ascii=False)
    with open(OUTPUT_DEMAND_MAP, "w", encoding="utf-8") as f:
        json.dump(mapped_demands, f, indent=4, ensure_ascii=False)
    coverage_summary(mapped_demands, microbundle_list)
    # Write original demand file with MicroCluster column
    write_demands_with_microcluster(
        INPUT_CSV_PATH,
        mapped_demands,
        OUTPUT_YTD_WITH_CLUSTERS,
    )
    print("Pipeline complete.")

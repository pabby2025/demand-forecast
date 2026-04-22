import optuna
import numpy as np
from data_loader import load_data
from skill_graph import build_skill_graph
from embeddings import generate_embeddings
from clustering import cluster_skills
from microbundle import generate_microbundles, select_final_microbundles
from config import SEED, OPTUNA_STUDY_NAME, DATABASE_URL


# Ensure consistent random seed in Optuna tuning
def objective(trial):
    SEED = 42  # Fixed seed for reproducibility

    # Embedding hyperparameters
    dim = trial.suggest_categorical("dim", [16, 32, 64])
    walk_length = trial.suggest_int("walk_length", 10, 40, step=5)
    window = trial.suggest_int("window", 5, 15)
    p = trial.suggest_float("p", 0.5, 2.0)
    q = trial.suggest_float("q", 0.5, 2.0)
    epochs = trial.suggest_int("epochs", 10, 30)

    # Clustering hyperparameters
    min_k = trial.suggest_int("min_k", 5, 20)
    k_step = trial.suggest_int("k_step", 2, 10)
    batch_size = trial.suggest_categorical("batch_size", [16, 32, 64])
    n_init = trial.suggest_int("n_init", 1, 10)
    max_iter = trial.suggest_int("max_iter", 100, 500, step=50)

    # Microbundle hyperparameters
    coh_threshold = 0.5
    target_total = 175
    min_overlap = 2

    # Load and preprocess data
    df = load_data()
    G, support = build_skill_graph(df)
    n2v, item_names, embeddings = generate_embeddings(
        G,
        seed=SEED,
        dim=dim,
        walk_length=walk_length,
        window=window,
        p=p,
        q=q,
        epochs=epochs,
    )
    if embeddings.shape[0] < 2:
        return -1.0
    # Clustering
    clusters, representatives, skill_to_cluster, cluster_to_medoid = cluster_skills(
        embeddings,
        item_names,
        seed=SEED,
        min_k=min_k,
        k_step=k_step,
        batch_size=batch_size,
        n_init=n_init,
        max_iter=max_iter,
    )
    # Microbundles
    # generate_microbundles may not accept coh_threshold directly; pass only if supported
    try:
        microbundles_by_cluster = generate_microbundles(clusters, G, n2v)
    except TypeError:
        microbundles_by_cluster = generate_microbundles(clusters, G, n2v)
    final_microbundles = select_final_microbundles(
        microbundles_by_cluster,
        clusters,
        df,
        target_total=target_total,
        min_overlap=min_overlap,
    )
    # Evaluate clustering quality (silhouette score) with sampling logic from clustering.py
    from sklearn.metrics import silhouette_score

    rng = np.random.default_rng(SEED)
    labels = [skill_to_cluster.get(name, -1) for name in item_names]
    sample_size = min(300, len(item_names))
    try:
        if len(item_names) > sample_size:
            sample_idx = rng.choice(len(item_names), size=sample_size, replace=False)
            score = silhouette_score(
                embeddings[sample_idx], np.array(labels)[sample_idx]
            )
        else:
            score = silhouette_score(embeddings, labels)
    except Exception:
        score = -1.0
    return score


def run_optuna_tuning(n_trials=30, study_name=OPTUNA_STUDY_NAME, storage=None):
    if storage:
        study = optuna.create_study(
            direction="maximize",
            study_name=study_name,
            storage=storage,
            load_if_exists=True,
        )
    else:
        study = optuna.create_study(direction="maximize")
        # Always use persistent SQLite storage for consistency with main.py
        storage = DATABASE_URL
        study = optuna.create_study(
            direction="maximize",
            study_name=study_name,
            storage=storage,
            load_if_exists=True,
        )
    study.optimize(objective, n_trials=n_trials)  # type: ignore
    print("Best trial:")
    print(study.best_trial)
    print("Best params:")
    print(study.best_params)
    return study


if __name__ == "__main__":
    run_optuna_tuning()

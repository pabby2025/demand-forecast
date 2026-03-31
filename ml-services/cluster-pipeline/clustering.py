# Added fixed random seed for reproducibility
from sklearn.cluster import MiniBatchKMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler
import numpy as np


def cluster_skills(
    embeddings,
    item_names,
    seed=42,
    scale=False,
    sample_for_silhouette=True,
    min_k=10,
    max_k=None,
    k_step=8,
    batch_size=32,
    n_init=1,
    max_iter=300,
):
    """
    Clusters items using MiniBatchKMeans, selects best k by silhouette score,
    and returns clusters, representatives (medoids), skill->cluster, and cluster->medoid.

    Arguments:
        embeddings (np.ndarray): shape (n_samples, n_features)
        item_names (list[str]): list of names aligned to embeddings
        seed (int): random seed for reproducibility
        scale (bool): whether to standardize features before clustering
        sample_for_silhouette (bool): if True, uses a fixed-seed sample up to 300 points;
        otherwise computes silhouette on full set.
    """
    embeddings = np.asarray(embeddings)
    assert (
        len(item_names) == embeddings.shape[0]
    ), "item_names must align with embeddings rows"

    # 1) Optional scaling to improve stability
    if scale:
        scaler = StandardScaler()
        X = scaler.fit_transform(embeddings)
    else:
        X = embeddings

    # 2) Candidate k values
    if max_k is None:
        max_k_bound = min(80, len(item_names))
    else:
        max_k_bound = min(max_k, len(item_names))
    k_options = list(range(min_k, max_k_bound + 1, k_step))

    # 3) Fixed-seed RNG for any sampling to ensure determinism
    rng = np.random.default_rng(seed)

    best_k, best_score = None, -1.0
    best_labels, best_centers = None, None

    # 4) Evaluate silhouette for each k
    for k in k_options:
        mbk = MiniBatchKMeans(
            n_clusters=k,
            batch_size=batch_size,
            n_init=n_init,
            random_state=seed,  # fixes init + minibatch sequence
            max_iter=max_iter,
        )
        labels = mbk.fit_predict(X)

        # Determine evaluation subset deterministically
        if sample_for_silhouette:
            sample_size = min(300, len(item_names))
            if len(item_names) > sample_size:
                sample_idx = rng.choice(
                    len(item_names), size=sample_size, replace=False
                )
                score = silhouette_score(X[sample_idx], labels[sample_idx])
            else:
                score = silhouette_score(X, labels)
        else:
            score = silhouette_score(X, labels)

        if score > best_score:
            best_score = score
            best_k = k
            best_labels = labels
            best_centers = mbk.cluster_centers_

    print(f"Best n_clusters: {best_k} and Best Silhouette Score: {best_score:.4f}")

    # Ensure best_k, best_labels, and best_centers are not None
    if best_k is None or best_labels is None or best_centers is None:
        raise ValueError("Clustering failed: No valid clusters were found.")

    labels = best_labels
    centers = best_centers

    # 5) Compute medoids (closest points to centers)
    representatives = []
    clusters = {}

    for c in range(best_k):
        members = np.where(labels == c)[0]
        if len(members) == 0:
            continue
        vecs = X[members]
        # Euclidean distance to center
        d = np.linalg.norm(vecs - centers[c], axis=1)
        medoid_idx = members[np.argmin(d)]
        medoid = item_names[medoid_idx]
        representatives.append(medoid)
        clusters[c] = {
            "members": [item_names[i] for i in members],
            "medoid": medoid,
        }

    skill_to_cluster = {item_names[i]: int(labels[i]) for i in range(len(item_names))}
    cluster_to_medoid = {c: clusters[c]["medoid"] for c in clusters}
    return clusters, representatives, skill_to_cluster, cluster_to_medoid

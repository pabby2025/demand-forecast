import networkx as nx
import itertools
import numpy as np

"""
Config values are only used as fallback defaults. All pipeline logic should use parameters passed from main.py/Optuna.
"""
import uuid


def avg_pairwise_cosine(skills, wv):
    vecs = [wv[s] for s in skills if s in wv.key_to_index]
    if len(vecs) < 2:
        return 0.0
    mat = np.vstack(vecs)
    mat = mat / np.linalg.norm(mat, axis=1, keepdims=True)
    sims = mat @ mat.T
    n = sims.shape[0]
    return (sims.sum() - np.trace(sims)) / (n * (n - 1))


def generate_microbundles(clusters, G, n2v):
    # import logging

    # logging.basicConfig(level=logging.INFO)

    def _get_config_value(val, default):
        return val if val is not None else default

    try:
        from config import COH_THRESHOLD as CFG_COH_THRESHOLD
    except ImportError:
        CFG_COH_THRESHOLD = 0.5

    def _generate_for_cluster(members, subG, n2v, coh_threshold):
        bundles = []
        triangles = [set(t) for t in nx.enumerate_all_cliques(subG) if len(t) == 3]
        for tri in triangles:
            coh = avg_pairwise_cosine(tri, n2v.wv)
            if coh >= coh_threshold:
                supp = 0.0
                for u, v in itertools.combinations(tri, 2):
                    if subG.has_edge(u, v):
                        supp += subG[u][v]["weight"]
                bundles.append({"bundle": frozenset(tri), "support": supp, "coh": coh})
        for b in list(bundles):
            base = set(b["bundle"])
            cand = set.intersection(*[set(subG.neighbors(s)) for s in base]) - base
            for x in cand:
                ext = base | {x}
                coh = avg_pairwise_cosine(ext, n2v.wv)
                if 3 < len(ext) <= 5 and coh >= coh_threshold:
                    supp = sum(
                        subG[u][v]["weight"]
                        for u, v in itertools.combinations(ext, 2)
                        if subG.has_edge(u, v)
                    )
                    bundles.append(
                        {"bundle": frozenset(ext), "support": supp, "coh": coh}
                    )
        return bundles

    def generate_microbundles_param(clusters, G, n2v, coh_threshold=None):
        coh_threshold = _get_config_value(coh_threshold, CFG_COH_THRESHOLD)
        # logging.info(f"Generating microbundles with coh_threshold={coh_threshold}")
        microbundles_by_cluster = {}
        # logging.info(f"Number of clusters: {len(clusters)}")
        for c, info in clusters.items():
            members = info["members"]
            subG = G.subgraph(members).copy()
            bundles = _generate_for_cluster(members, subG, n2v, coh_threshold)
            # logging.info(
            #    f"Cluster {c}: {len(members)} members, {len(bundles)} candidate bundles"
            # )

            def jaccard(a, b):
                return len(a & b) / max(1, len(b))

            bundles.sort(key=lambda x: (x["coh"], x["support"]), reverse=True)
            selected, seen = [], []
            for b in bundles:
                s = set(b["bundle"])
                if any(jaccard(s, t) >= 0.70 for t in seen):
                    continue
                selected.append(
                    {"bundle": b["bundle"], "support": b["support"], "coh": b["coh"]}
                )
                seen.append(s)
            # logging.info(f"Cluster {c}: {len(selected)} selected microbundles")
            microbundles_by_cluster[c] = selected
        return microbundles_by_cluster

    return generate_microbundles_param(clusters, G, n2v)


def select_final_microbundles(
    microbundles_by_cluster, clusters, df, target_total=None, min_overlap=None
):
    # import logging

    # logging.basicConfig(level=logging.INFO)
    # User override: always use min_overlap=2 and target_total=175 unless explicitly passed
    DEFAULT_TARGET_TOTAL = 175
    DEFAULT_MIN_OVERLAP = 2
    # target_total = target_total if target_total is not None else DEFAULT_TARGET_TOTAL
    # min_overlap = min_overlap if min_overlap is not None else DEFAULT_MIN_OVERLAP

    target_total = DEFAULT_TARGET_TOTAL
    min_overlap = DEFAULT_MIN_OVERLAP

    # logging.info(
    #    f"select_final_microbundles: target_total={target_total}, min_overlap={min_overlap}"
    # )
    demands_sets = [set(sk) for sk in df["tsr"].tolist()]
    total_demands = len(demands_sets)
    demand_idx_by_cluster = {c: [] for c in clusters}
    for i, d in enumerate(demands_sets):
        touched = set([c for s in d for c in clusters if s in clusters[c]["members"]])
        for c in touched:
            demand_idx_by_cluster[c].append(i)

    def covers(bundle, demand, min_overlap=min_overlap):
        return len(bundle & demand) >= min_overlap

    def greedy_select_bundles_for_cluster(
        candidates, demand_indices, demands_sets, M_c=8, min_overlap=min_overlap
    ):
        selected = []
        uncovered = set(demand_indices)
        cov_sets = []
        for cand in candidates:
            b = set(cand["bundle"])
            cov = {i for i in demand_indices if covers(b, demands_sets[i], min_overlap)}
            cov_sets.append(cov)
        while len(selected) < M_c and uncovered:
            best_i, best_gain, best_support = None, -1, -1.0
            for i, cand in enumerate(candidates):
                if i in selected:
                    continue
                gain = len(cov_sets[i] & uncovered)
                supp = float(cand.get("support", 0.0))
                if gain > best_gain or (gain == best_gain and supp > best_support):
                    best_i, best_gain, best_support = i, gain, supp
            if best_i is None or best_gain <= 0:
                break
            selected.append(best_i)
            uncovered -= cov_sets[best_i] & uncovered
        return [candidates[i]["bundle"] for i in selected]

    final_microbundles = {}
    for c in clusters:
        share = len(demand_idx_by_cluster[c]) / max(1, total_demands)
        M_c = max(3, int(target_total * share))
        candidates = microbundles_by_cluster.get(c, [])
        # logging.info(
        #    f"Cluster {c}: {len(candidates)} candidate bundles before selection (M_c={M_c})"
        # )
        if not candidates:
            final_microbundles[c] = []
            continue
        selected_bundles = greedy_select_bundles_for_cluster(
            candidates,
            demand_idx_by_cluster[c],
            demands_sets,
            M_c=M_c,
            min_overlap=min_overlap,
        )
        # logging.info(
        #    f"Cluster {c}: {len(selected_bundles)} bundles selected after filtering"
        # )
        # if len(selected_bundles) > 0:
        # logging.info(f"Cluster {c}: Sample selected bundle: {selected_bundles[0]}")
        final_microbundles[c] = selected_bundles
    return final_microbundles


def name_microbundles(final_microbundles):
    microbundle_list = []
    microbundle_skill_sets = []
    for c, bundles in final_microbundles.items():
        for skills in bundles:
            name = f"MB-{'-'.join(sorted(list(skills))[:3])}-{str(uuid.uuid4())[:8]}"
            microbundle_list.append({"name": name, "skills": sorted(list(skills))})
            microbundle_skill_sets.append((name, set(skills)))
    return microbundle_list, microbundle_skill_sets

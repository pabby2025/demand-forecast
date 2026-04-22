import networkx as nx
from collections import defaultdict
from itertools import combinations
import math
import numpy as np
from config import MIN_SUPPORT, TOP_K, SIM_THRESHOLD, SIM_METRIC


def build_skill_graph(df):
    support = defaultdict(int)
    pair_counts = defaultdict(int)
    for skills in df["tsr"]:
        uniq = sorted(set(skills))
        for s in uniq:
            support[s] += 1
        for a, b in combinations(uniq, 2):
            pair_counts[(a, b)] += 1
    valid_skills = {s for s, c in support.items() if c >= MIN_SUPPORT}
    print(
        f"Total Skills: {len(support)} and Valid Skills (having atleast {MIN_SUPPORT} count: {len(valid_skills)}"
    )

    def jacc(co, sup_a, sup_b):
        denom = sup_a + sup_b - co
        return co / denom if denom > 0 else 0.0

    def lift(co, sup_a, sup_b, n):
        pij = co / n
        pi, pj = sup_a / n, sup_b / n
        return pij / (pi * pj) if pi > 0 and pj > 0 else 0.0

    def ppmi(co, sup_a, sup_b, n):
        pij = co / n
        pi, pj = sup_a / n, sup_b / n
        val = math.log(pij / (pi * pj)) if pi > 0 and pj > 0 and pij > 0 else -math.inf
        return max(0.0, val) if math.isfinite(val) else 0.0

    def compute_sim(metric, co, sa, sb, n):
        if metric == "jaccard":
            return jacc(co, sa, sb)
        if metric == "lift":
            return lift(co, sa, sb, n)
        if metric == "ppmi":
            return ppmi(co, sa, sb, n)
        return 0.0

    def graph_stats(G):
        degs = [d for _, d in G.degree()]
        avg_deg = np.mean(degs) if degs else 0
        lcc = max(nx.connected_components(G), key=len) if G.number_of_nodes() else set()
        lcc_ratio = len(lcc) / max(1, G.number_of_nodes())
        return {
            "avg_degree": avg_deg,
            "lcc_ratio": lcc_ratio,
            "num_nodes": G.number_of_nodes(),
            "num_edges": G.number_of_edges(),
        }

    n_demands = len(df["tsr"])
    neighbors = {s: [] for s in valid_skills}
    for (a, b), co in pair_counts.items():
        if a not in valid_skills or b not in valid_skills:
            continue
        sup_a, sup_b = support[a], support[b]
        sim = compute_sim(SIM_METRIC, co, sup_a, sup_b, n_demands)
        if sim >= SIM_THRESHOLD:
            neighbors[a].append((b, sim))
            neighbors[b].append((a, sim))
    for s in list(neighbors.keys()):
        if not neighbors[s]:
            del neighbors[s]
            continue
        neighbors[s].sort(key=lambda x: x[1], reverse=True)
        neighbors[s] = neighbors[s][:TOP_K]
    G = nx.Graph()
    for s in neighbors.keys():
        G.add_node(s, support=support[s])
    for a, neighs in neighbors.items():
        for b, w in neighs:
            if a == b:
                continue
            if G.has_edge(a, b):
                G[a][b]["weight"] = max(G[a][b]["weight"], float(w))
            else:
                G.add_edge(a, b, weight=float(w))

    stats = graph_stats(G)
    print(
        f"Avg degree={stats['avg_degree']:.2f}, LCC ratio={stats['lcc_ratio']:.2f}, nodes={stats['num_nodes']}, edges={stats['num_edges']}"
    )

    return G, support

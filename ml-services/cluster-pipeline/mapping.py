import numpy as np
from config import MIN_JACCARD


def map_demands_to_microbundles(df, microbundle_skill_sets, G=None, alpha=0.5):
    """
    Map demands to microbundles using Jaccard and graph connectivity (edge weights).
    alpha: weight for Jaccard (0-1), (1-alpha): weight for graph score.
    """
    mapped_demands = []
    for demand in df["tsr"]:
        d = set(demand)
        # 1. Try to find a single microbundle that fully covers the demand
        single_cover = None
        single_score = -1.0
        single_jaccard = 0.0
        single_graph_score = 0.0
        for name, skills in microbundle_skill_sets:
            if d.issubset(skills):
                jaccard = len(d & skills) / max(1, len(d))
                graph_score = 0.0
                edge_count = 0
                if G is not None:
                    for s1 in d:
                        for s2 in skills:
                            if s1 != s2 and G.has_edge(s1, s2):
                                graph_score += G[s1][s2].get("weight", 0.0)
                                edge_count += 1
                    norm = max(1, edge_count)
                    graph_score = graph_score / norm
                score = alpha * jaccard + (1 - alpha) * graph_score
                if score > single_score:
                    single_score = score
                    single_cover = (name, skills)
                    single_jaccard = jaccard
                    single_graph_score = graph_score
        if single_cover is not None and single_score >= MIN_JACCARD:
            mapped_demands.append(
                {
                    "demand_skills": demand,
                    "microbundle_names": [single_cover[0]],
                    "microbundle_skills": [sorted(list(single_cover[1]))],
                    "jaccards": [round(single_jaccard, 3)],
                    "graph_scores": [round(single_graph_score, 3)],
                    "combined_scores": [round(single_score, 3)],
                    "full_coverage": True,
                }
            )
            continue

        # # 2. Try to find two microbundles that together cover the demand
        # best_pair = None
        # best_pair_score = -1.0
        # best_pair_jaccards = (0.0, 0.0)
        # best_pair_graph_scores = (0.0, 0.0)
        # for i, (name1, skills1) in enumerate(microbundle_skill_sets):
        #     for j, (name2, skills2) in enumerate(microbundle_skill_sets):
        #         if i >= j:
        #             continue
        #         covered = skills1 | skills2
        #         if d.issubset(covered):
        #             jaccard1 = len(d & skills1) / max(1, len(d | skills1))
        #             jaccard2 = len(d & skills2) / max(1, len(d | skills2))
        #             graph_score1 = 0.0
        #             graph_score2 = 0.0
        #             edge_count1 = 0
        #             edge_count2 = 0
        #             if G is not None:
        #                 for s1 in d:
        #                     for s2_1 in skills1:
        #                         if s1 != s2_1 and G.has_edge(s1, s2_1):
        #                             graph_score1 += G[s1][s2_1].get("weight", 0.0)
        #                             edge_count1 += 1
        #                 for s1 in d:
        #                     for s2_2 in skills2:
        #                         if s1 != s2_2 and G.has_edge(s1, s2_2):
        #                             graph_score2 += G[s1][s2_2].get("weight", 0.0)
        #                             edge_count2 += 1
        #                 norm1 = max(1, edge_count1)
        #                 norm2 = max(1, edge_count2)
        #                 graph_score1 = graph_score1 / norm1
        #                 graph_score2 = graph_score2 / norm2
        #             score1 = alpha * jaccard1 + (1 - alpha) * graph_score1
        #             score2 = alpha * jaccard2 + (1 - alpha) * graph_score2
        #             avg_score = (score1 + score2) / 2
        #             if avg_score > best_pair_score:
        #                 best_pair_score = avg_score
        #                 best_pair = ((name1, skills1), (name2, skills2))
        #                 best_pair_jaccards = (jaccard1, jaccard2)
        #                 best_pair_graph_scores = (graph_score1, graph_score2)
        # if best_pair is not None and best_pair_score >= MIN_JACCARD:
        #     mapped_demands.append(
        #         {
        #             "demand_skills": demand,
        #             "microbundle_names": [best_pair[0][0], best_pair[1][0]],
        #             "microbundle_skills": [
        #                 sorted(list(best_pair[0][1])),
        #                 sorted(list(best_pair[1][1])),
        #             ],
        #             "jaccards": [
        #                 round(best_pair_jaccards[0], 3),
        #                 round(best_pair_jaccards[1], 3),
        #             ],
        #             "graph_scores": [
        #                 round(best_pair_graph_scores[0], 3),
        #                 round(best_pair_graph_scores[1], 3),
        #             ],
        #             "combined_scores": [
        #                 round(
        #                     (
        #                         alpha * best_pair_jaccards[0]
        #                         + (1 - alpha) * best_pair_graph_scores[0]
        #                     ),
        #                     3,
        #                 ),
        #                 round(
        #                     (
        #                         alpha * best_pair_jaccards[1]
        #                         + (1 - alpha) * best_pair_graph_scores[1]
        #                     ),
        #                     3,
        #                 ),
        #             ],
        #             "full_coverage": True,
        #         }
        #     )
        #     continue

        # 3. Fallback: best single microbundle (as before)
        best_score = -1.0
        best_name, best_skills, best_jaccard, best_graph_score = None, None, 0.0, 0.0
        for name, skills in microbundle_skill_sets:
            jaccard = len(d & skills) / max(1, len(d))
            graph_score = 0.0
            edge_count = 0
            if G is not None:
                for s1 in d:
                    for s2 in skills:
                        if s1 != s2 and G.has_edge(s1, s2):
                            graph_score += G[s1][s2].get("weight", 0.0)
                            edge_count += 1
                norm = max(1, edge_count)
                graph_score = graph_score / norm
            score = alpha * jaccard + (1 - alpha) * graph_score
            if score > best_score:
                best_score = score
                best_name = name
                best_skills = skills
                best_jaccard = jaccard
                best_graph_score = graph_score
        if best_score >= MIN_JACCARD:
            mapped_demands.append(
                {
                    "demand_skills": demand,
                    "microbundle_names": [best_name],
                    "microbundle_skills": [sorted(list(best_skills))],  # type: ignore
                    "jaccards": [round(best_jaccard, 3)],
                    "graph_scores": [round(best_graph_score, 3)],
                    "combined_scores": [round(best_score, 3)],
                    "full_coverage": False,
                }
            )
        else:
            mapped_demands.append(
                {
                    "demand_skills": demand,
                    "microbundle_names": [],
                    "microbundle_skills": [],
                    "jaccards": [],
                    "graph_scores": [],
                    "combined_scores": [],
                    "full_coverage": False,
                }
            )
    return mapped_demands


def coverage_summary(mapped_demands, microbundle_list):
    fully_covered = sum(1 for m in mapped_demands if m["full_coverage"])
    coverage = sum(
        1
        for m in mapped_demands
        if m["microbundle_names"] is not None and len(m["microbundle_names"]) > 0
    )
    total = len(mapped_demands)
    print(f"Demand coverage: {fully_covered}/{total} ({fully_covered/total:.1%})")
    print(f"At least one microbundle mapped: {coverage}/{total} ({coverage/total:.1%})")
    print(f"Total microbundles: {len(microbundle_list)}")


import pandas as pd


def write_demands_with_microcluster(input_csv_path, mapped_demands, output_csv_path):
    df = pd.read_csv(input_csv_path)
    microcluster_col = [" | ".join(m["microbundle_names"]) for m in mapped_demands]
    df["Skill Cluster"] = microcluster_col
    df_out = df.dropna(subset=["Skill Cluster"])
    df_out.to_csv(output_csv_path, encoding="utf-8", index=False)
    print(
        f"Wrote demands with Skill Cluster to {output_csv_path}, {len(df_out)}/{len(df)} rows retained after dropping empty clusters."
    )

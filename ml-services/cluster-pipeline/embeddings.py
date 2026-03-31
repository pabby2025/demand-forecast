import numpy as np
from fastnode2vec import Graph, Node2Vec as FastNode2Vec


def generate_embeddings(
    G, seed=42, dim=32, walk_length=20, window=10, p=1.0, q=1.5, workers=4, epochs=20
):
    edges = []
    for u, v, data in G.edges(data=True):
        w = float(data["weight"])
        edges.append((u, v, w))
        edges.append((v, u, w))
    fgraph = Graph(edges, directed=True, weighted=True)
    n2v = FastNode2Vec(
        fgraph,
        dim=dim,
        walk_length=walk_length,
        window=window,
        p=p,
        q=q,
        workers=workers,
        seed=seed,
    )
    n2v.train(epochs=epochs)
    item_names = [n for n in G.nodes() if n in n2v.wv.key_to_index]
    if item_names:
        embeddings = np.vstack([n2v.wv[n] for n in item_names])
        embeddings = embeddings / (
            np.linalg.norm(embeddings, axis=1, keepdims=True) + 1e-12
        )
    else:
        embeddings = np.empty((0, dim))
    return n2v, item_names, embeddings

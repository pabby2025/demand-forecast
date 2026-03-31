# OneC_4898_DemandForecasting

## Setup

### 1. Create a virtual environment

**Windows:**
```bat
python -m venv .venv
.venv\Scripts\activate
```

**Linux:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

> Note: `autogluon.tabular` is a large package (~2 GB). Installation may take several minutes.

---

## Flow

1. PreProcess (additional 1 time step LLM Based normalization)
2. Skill_normalized
3. Skill_clusters_Demand
4. Human inputs to `skills\Americas_DE\skill_clusters.json` and then run the apply_clusters

## Training and inferencing

1. data_split
2. Build_training_groups
3. train_and_predict
4. Ssd Guardrail

Instead you can directly run `run_pipeline.sh` !

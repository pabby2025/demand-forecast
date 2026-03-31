# Demand Planning — Architecture Summary

## System Context

```
[SL COO] [Market COO] [Central Forecast Planner]
         |                |                |
         v                v                v
    ┌─────────── React.js SPA ───────────────┐
    │  Tailwind CSS + TanStack + Conv. UI     │
    └──────────────┬──────────────────────────┘
                   │ HTTPS/TLS 1.2
                   v
    ┌──────── Apigee API Gateway ─────────────┐
    └──────────────┬──────────────────────────┘
                   │
    ┌──────── Domain Services (Python) ───────┐
    │ Demand Mgmt │ Supply Mgmt │ Forecast/AI │
    │ Agentic Svc │ Notification Svc          │
    └──────────────┬──────────────────────────┘
                   │
    ┌──────── Azure ML Service ───────────────┐
    │ Training Jobs │ Inference │ Data ETL     │
    │ LLM (GPT 5.1/4.1) │ Embeddings │VectorDB│
    └──────────────┬──────────────────────────┘
                   │
    ┌──────── Data Layer ─────────────────────┐
    │ PostgreSQL │ PG Vector │ Azure Data Lake │
    │ Azure Blob Storage                       │
    └──────────────────────────────────────────┘
                   │
    ┌──────── External Systems ───────────────┐
    │ QuickSO │ WinZone │ UPT │ Price Builder │
    │ Wise │ Market Intelligence │ UPLF        │
    └──────────────────────────────────────────┘
```

## Service Boundaries

| Service | Responsibility | Key Entities |
|---------|---------------|--------------|
| Demand Management | Demand capture, breakdown, type analysis | Demand records, demand types, billability |
| Supply Management | Resource supply tracking, location mix | Supply records, locations, grade distribution |
| Forecasting & AI | ML model execution, forecast generation | Forecasts, scenarios, KPIs, explainability |
| Agentic Service | Intelligent automation, conversational AI | Agents, workflows, LLM orchestration |
| Notification Service | Alerts, task notifications, emails | Alerts, tasks, interlocks |

## Key Technology Decisions

1. **Python for backend** — Rich ML/AI ecosystem, seamless integration with data science
2. **React + Tailwind + TanStack** — Modern, performant frontend with excellent table handling
3. **Azure OpenAI (GPT 5.1/4.1 mini)** — Enterprise-grade LLM with function calling, JSON mode
4. **Qdrant (target) / pgvector (interim)** — Purpose-built vector DB for RAG architecture
5. **PostgreSQL** — Robust relational DB with vector extension support
6. **Apigee** — Enterprise API management gateway
7. **Azure ML Service** — Managed ML compute for training and inference

## Migration Context (from TMP AS-IS)

The Demand Planning module is a **greenfield build** within the TMP modernization.
Unlike TMP core (which reuses/wraps .NET WCF services), Demand Planning is:
- Built entirely with the **modern stack** (Python + React)
- Hosted on **Azure** (with eventual GCP migration path)
- Designed as **domain microservices** from day one
- Integrated with the broader TMP ecosystem via REST APIs

# 🏗️ Lakehouse-DQ-Pipeline — Data Quality Framework

<p align="center">
  <img src="https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="PySpark"/>
  <img src="https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=delta&logoColor=white" alt="Delta Lake"/>
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white" alt="Databricks"/>
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/pytest-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white" alt="pytest"/>
</p>

## 🎯 Project Overview

E-commerce data pipeline with a **Bronze/Silver/Gold Lakehouse architecture** and a **custom Data Quality framework** built on PySpark. Features a quarantine pattern for rejected records and generates HTML DQ reports for stakeholder visibility.

## 🏗️ Architecture

```mermaid
flowchart TB
    subgraph Source ["📦 Data Source"]
        CSV[CSV Transaction Files\n50K+ e-commerce records]
    end

    subgraph Bronze ["🥉 Bronze Layer"]
        BI[Raw Ingestion\nAppend-only to Delta Lake]
    end

    subgraph DQ ["🔍 Data Quality Engine"]
        direction TB
        C1[not_null checks]
        C2[unique checks]
        C3[positive checks]
        C4[range checks]
        C5[values_in_set checks]
        RPT[📊 HTML DQ Report]
    end

    subgraph Silver ["🥈 Silver Layer"]
        DEDUP[Deduplication]
        VALID[Validation & Flags]
        ENRICH[Enrichment]
    end

    subgraph Quarantine ["🚫 Quarantine"]
        QR[(Rejected Records\nfor Investigation)]
    end

    subgraph Gold ["🥇 Gold Layer"]
        G1[📈 Daily Revenue\nby Country & Category]
        G2[🛍️ Product Performance\nReturn Rate & Revenue]
        G3[👥 Customer LTV\nSegmentation & CLV]
    end

    CSV --> BI
    BI --> DQ
    DQ --> Silver
    DQ --> Quarantine
    C1 & C2 & C3 & C4 & C5 --> RPT
    Silver --> DEDUP --> VALID --> ENRICH
    ENRICH --> Gold
    Gold --> G1 & G2 & G3

    style BI fill:#CD7F32,color:#fff
    style Silver fill:#C0C0C0,color:#000
    style Gold fill:#FFD700,color:#000
    style Quarantine fill:#dc3545,color:#fff
```

## 🚀 Features

- 🏗️ **Lakehouse architecture** with Bronze/Silver/Gold layers on Delta Lake
- 🔍 **Custom DQ framework** with 5 check types: `not_null`, `unique`, `positive`, `range`, `values_in_set`
- 🚫 **Quarantine pattern** — rejected records stored separately (zero data loss)
- 📊 **HTML DQ reports** — auto-generated pass/fail scorecards for stakeholders
- 🔄 **Idempotent** processing via Delta Lake MERGE upsert
- 👥 **Customer segmentation** — VIP / Regular / Occasional / One-time based on LTV

## 🔍 Data Quality Framework

| Check Type | Description | Example |
|-----------|-------------|---------|
| `not_null` | Column completeness rate | `transaction_id` >= 99% non-null |
| `unique` | Column uniqueness rate | `transaction_id` >= 98% unique |
| `positive` | Numeric positivity | `quantity` > 0 for >= 95% of rows |
| `range` | Numeric boundaries | `unit_price` between 0.01 and 10,000 |
| `values_in_set` | Categorical validation | `payment_method` in {credit_card, paypal, ...} |

Each check returns a `DQResult` dataclass with pass/fail status, metric, threshold, and details.

## 📂 Project Structure

```
Lakehouse-DQ-Pipeline/
├── 📁 notebooks/
│   ├── 01_bronze_ingestion.py        # Raw CSV to Delta Lake
│   ├── 02_silver_cleansing.py        # Dedup + DQ + Quarantine + Enrich
│   └── 03_gold_aggregation.py        # Revenue, Products, Customer LTV
├── 📁 src/
│   ├── data_quality/
│   │   ├── expectations.py           # DQ check functions
│   │   └── dq_report.py              # HTML report generator
│   └── generators/
│       └── ecommerce_generator.py    # Mock data with controlled errors
├── 📁 tests/
│   └── test_dq_checks.py            # 7 unit tests for DQ functions
├── 📁 config/
│   └── pipeline_config.yaml          # YAML-driven DQ thresholds
└── requirements.txt
```

## ⚙️ Setup & Run

```bash
pip install -r requirements.txt
python src/generators/ecommerce_generator.py
pytest tests/ -v
```

## 👨‍💻 Author

**Othmane Sadiki** — Data Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/sadiki-othmane/)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/othmanestd)

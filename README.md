# Lakehouse-DQ-Pipeline

## Overview
E-commerce data pipeline with a **Bronze/Silver/Gold Lakehouse architecture**
and a custom **Data Quality framework** built on PySpark and Delta Lake.

## Architecture
![Architecture](architecture/architecture.png)

**Pipeline Flow:**
1. **Raw** - CSV transaction files land in Azure Data Lake
2. **Bronze** - Raw ingestion to Delta Lake (append-only, no transforms)
3. **DQ Checks** - Custom expectation-based validation framework
4. **Quarantine** - Rejected records stored separately for investigation
5. **Silver** - Cleaned, deduplicated, enriched transactions
6. **Gold** - Business aggregations: daily revenue, product performance, customer LTV

## Tech Stack
| Component | Technology |
|-----------|-----------|
| Processing | Databricks, Apache Spark 3.x, PySpark |
| Storage | Azure Data Lake Gen2, Delta Lake |
| Data Quality | Custom expectation framework (inspired by Great Expectations) |
| Testing | pytest + PySpark local mode |
| Language | Python, SQL |

## Data Quality Framework

The DQ framework provides configurable checks that run at the Silver layer:

| Check Type | Description | Example |
|-----------|-------------|---------|
| `not_null` | Column completeness | transaction_id >= 99% non-null |
| `unique` | Column uniqueness | transaction_id >= 98% unique |
| `positive` | Numeric positivity | quantity > 0 for >= 95% of rows |
| `values_in_set` | Categorical validation | payment_method in {credit_card, paypal, ...} |
| `range` | Numeric range | unit_price between 0.01 and 10000 |

Each check returns a `DQResult` with pass/fail status, metric value, and details.
Failed records are quarantined, not dropped.

## Project Structure
```
Lakehouse-DQ-Pipeline/
|-- notebooks/
|   |-- 01_bronze_ingestion.py
|   |-- 02_silver_cleansing.py
|   +-- 03_gold_aggregation.py
|-- src/
|   |-- data_quality/
|   |   |-- expectations.py      # DQ check functions
|   |   +-- dq_report.py         # HTML report generator
|   +-- generators/
|       +-- ecommerce_generator.py
|-- tests/
|   +-- test_dq_checks.py
|-- config/
|   +-- pipeline_config.yaml
|-- data/sample/
|-- docs/
|   +-- interview_notes.md
+-- requirements.txt
```

## Setup & Run
```bash
pip install -r requirements.txt
python src/generators/ecommerce_generator.py
pytest tests/ -v
```

## Gold Layer Outputs

**Daily Revenue:** revenue by country and category, order count, AOV, unique customers
**Product Performance:** times sold, total revenue, return rate, unique buyers
**Customer LTV:** lifetime value, segment (VIP/Regular/Occasional/One-time), purchase history

## Key Design Decisions
- Quarantine pattern instead of silent drops (no data loss)
- DQ checks are config-driven (YAML thresholds, easy to adjust)
- Partition by month (not day) to avoid small file problem on moderate data volumes
- MERGE upsert at Silver for idempotent reprocessing

## Author
**Othmane Sadiki** - [LinkedIn](https://www.linkedin.com/in/sadiki-othmane/) - othmanesadiki6114@gmail.com

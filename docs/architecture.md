# Architecture

```mermaid
flowchart LR
    A[Python Chaos Simulator] -->|daily Parquet files| B[data/ folder]
    B -->|PUT + COPY INTO| C[(Snowflake RAW)]
    C -->|dbt staging| D[(STAGING)]
    D -->|dbt snapshot + marts| E[(MARTS: star schema)]
    F[Airflow DAG] -.orchestrates.-> A
    F -.-> C
    F -.-> D
    F -.-> E
```

## Layers
- **RAW**: data exactly as received, plus load metadata
- **STAGING**: cleaned, typed, deduplicated
- **MARTS**: fct_order_items, dim_customer (SCD2), dim_product, dim_date

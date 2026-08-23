
### Pipeline 1 (ETL, Automation)


```graphql
                   Apache Airflow / MWAA
                ─────── ORCHESTRATION ───────
                     (scheduled execution)
                              │
                              │
extract ──────────────────────┼──────────────────────────────────
                              │
                              │       ┌─────────────────┐
                              └──────►│   PostgreSQL    │
                                      └────────┬────────┘
                                               │
                                               ▼
transform ───────────────────────────────────────────────────────
light analytical batch processing     ┌─────────────────┐
                                      │ DuckDB + Python │
                                      │                 │
                                      │  Entity Resolve │
                                      │  + Transform    │
                                      └────────┬────────┘
                                               │
                                               ▼
load ─────────────────────────────────────────────────────────────
cheap columnar storage                ┌─────────────────┐
                                      │       S3        │
                                      │     Parquet     │
                                      └────────┬────────┘
                                               │
                                               ▼
deliver ──────────────────────────────────────────────────────────
                                               └──────► Salesforce
```



### Entity Resolution 

```graphql
                  Missing Primary ID
                         │
                         ▼
              ┌─────────────────────┐
              │   Resolution Rule   │
              │                     │
              │   Exact Identifier  │
              │        Match        │
              └──────────┬──────────┘
                         │
                    Match found?
                    ┌────┴────┐
                   YES        NO
                    │          │
                    ▼          ▼
              ┌──────────┐   ┌──────────────────────┐
              │ RESOLVED │   │     Next Rule        │
              └────┬─────┘   │ Alternate Identifier │
                   │         └──────────┬───────────┘
                   │                    │
                   │               Match found?
                   │               ┌────┴────┐
                   │              YES        NO
                   │               │          │
                   └───────────────┤          ▼
                                   │     QUARANTINED
                                   ▼
		                       ┌──────────────────┐
		                       │     Record       │
		                       │  Reconstructed   │
		                       └──────────────────┘
```

#### Table - Stage Responsibilities

| **Layer**                       | **Technology**      | **Responsibility**                                                                    |
| ---------------------------- | --------------- | --------------------------------------------------------------------------------- |
| **Source**                   | PostgreSQL      | Raw data & source of extraction                   |
| **Storage**                  | S3 + Parquet    | Columnar, low-cost storage for pipeline outputs & downstream consumption. |
| **Compute / Transformation** | DuckDB + Python | Batch transformations, data normalization, & entity resolution    |
| **Orchestration**            | Airflow / MWAA  | Scheduling, task dependencies, retries, & execution of the pipeline             |
| **Delivery**                 | Salesforce      | Receives the final reconstructed usage data for downstream reporting              |


#### Table - Tradeoffs

| **Decision** | **Why** | **Tradeoff** |
| --- | --- | --- |
| Exact entity matching | Incorrect assignments were high-risk | Lower recovery rate |
| DuckDB in MWAA | No new compute infrastructure | Compute & orchestration are coupled |
| Parquet on S3 | Cheap analytical storage + reruns | File management |
| Batch processing | Business process was monthly | Not suitable for more frequent use |



### Pipeline 2 (Analytics)

```graphql
    Modular Architecture
		   │
		   │
		 extract: configuration-driven, environment agnostic
		   │       ┌───────────┐ 
		   ├───────┤  GraphQL  │
		   │       └───────────┘
		   ▼              
		  load: idempotent load, raw_schema 
		   │       ┌────────────┐ 
		   ├───────┤  Postgres  │ 
		   │       └────────────┘
		   ▼ 
		transform: staging, marts
		   │       ┌───────┐ 
		   ├───────┤  dbt  │
		   │       └───────┘
		   ▼  
		 deliver: governed analytics
		   └─────────►┬──────┐     ┌─────────────────────────────────┐
			          │  S3  │  +  │ Analytics Platform Applications │
			          └──────┘     └─────────────────────────────────┘
```




```graphql
Modular Architecture

                   Configuration-Driven
                  Multi-Tenant Ingestion
                           │
                           │
 extract ──────────────────┼────────────────────────┐
 (environment-agnostic)    │                        │
                           │                  ┌──────────────┐
                           └─────────────────►│ Multi-Tenant │
                                              │ GraphQL APIs │
                                              └──────────────┘
                           ▼
 load ───────────────────────────────────────────────────────
 Windowed idempotent loading
 (safe reruns & historical backfills)
                           │
                           │                  ┌────────────┐
                           └─────────────────►│  Postgres  │
                                              └────────────┘
                           ▼
 transform ─────────────────────────────────────────────────
 Domain-oriented models
 (raw → staging → marts)
                           │
                           │                  ┌────────┐
                           └─────────────────►│  dbt   │
                                              └────────┘
                           ▼
 deliver ───────────────────────────────────────────────────
 Governed analytics
 Independent job execution
 (tenant failure isolation)
                           └────────►┌──────┐     ┌──────────────────────────┐
                                     │  S3  │  +  │ Analytics Platform Apps  │
                                     └──────┘     └──────────────────────────┘


```

# Architecture - 1

```graphql
# Environment-Agnostic

PIPELINE_JOBS = [ 
#jobs run independently, order doesnt matter
  # ENV 1
	(env, mv_key), # each tuple is (environment, materialized_view)
	(env, mv_key),
	...
	# ENV 2
	...
]
```

# Architecture - 2
```graphql
EXTRACT → LOAD → TRANSFORM → DELIVER
```



# Architecture - 3

```graphql
#delete and reload scoped to the fetched date window 
...
conn.execute(text(f'DELETE FROM "{RAW_SCHEMA}"."{table}" 
					WHERE "{date_col}" >= :start AND "{date_col}" < :end'))
...
```


# Architecture - 4

```graphql
# Running the extract module 
run_all(...):
			"""Runs every job, isolating failures. """
...		   
for env, mv in jobs:
		try: 
				table = run_one(env, mv, start_date, end_date)
				results.append({... "status": "ok", ...})
			
		except Exception as e: "One job's failure will not take down the rest"
				results.append({... "status": "error", "detail":str(e)})
```

# Architecture - 5
```graphql

					raw   →  staging  →  marts 
							       ▼  
						 domain separation
					(content, usage, sessions)
```

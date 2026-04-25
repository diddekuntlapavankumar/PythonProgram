# Retail Medallion DLT Pipeline

This repository contains a Databricks Delta Live Tables pipeline for a retail medallion architecture.

## Layers

- **Bronze**: Raw Auto Loader ingestion for `customers`, `products`, `stores`, `orders`, `order_items`, and `inventory`.
- **Silver**: Type casting, trimming, normalization, deduplication, and DLT data-quality expectations.
- **Gold**: Reporting-ready dimensions, a sales fact table, daily sales summary, customer lifetime value, and product performance.

## Raw Data Layout

Set the pipeline configuration value `retail.data_root` to the folder or Unity Catalog volume that contains these folders:

```text
customers/
products/
stores/
orders/
order_items/
inventory/
```

The default is:

```text
/Volumes/main/retail/raw
```

## Deploy

Update `resources/retail_dlt_pipeline.yml` for your target catalog, schema, and raw data location, then deploy with Databricks Asset Bundles:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run retail_medallion_pipeline -t dev
```

## Main Pipeline File

The Delta Live Tables definitions are in:

```text
dlt_pipelines/retail_medallion_pipeline.py
```

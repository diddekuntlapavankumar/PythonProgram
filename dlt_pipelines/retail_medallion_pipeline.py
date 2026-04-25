"""Delta Live Tables pipeline for a retail medallion architecture.

Pipeline settings expected in Databricks:
  - retail.data_root: root folder or volume containing raw retail data

Raw folders expected below retail.data_root:
  - customers/
  - products/
  - stores/
  - orders/
  - order_items/
  - inventory/
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    TimestampType,
)


DATA_ROOT = spark.conf.get("retail.data_root", "/Volumes/main/retail/raw")


def read_cloud_files(entity_name: str, file_format: str = "json"):
    """Read a raw retail entity with Auto Loader and add ingestion metadata."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{DATA_ROOT}/{entity_name}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_ts", F.current_timestamp())
    )


@dlt.table(
    name="bronze_customers",
    comment="Raw customer records ingested from retail source files.",
    table_properties={"quality": "bronze"},
)
def bronze_customers():
    return read_cloud_files("customers")


@dlt.table(
    name="bronze_products",
    comment="Raw product master records ingested from retail source files.",
    table_properties={"quality": "bronze"},
)
def bronze_products():
    return read_cloud_files("products")


@dlt.table(
    name="bronze_stores",
    comment="Raw store master records ingested from retail source files.",
    table_properties={"quality": "bronze"},
)
def bronze_stores():
    return read_cloud_files("stores")


@dlt.table(
    name="bronze_orders",
    comment="Raw order headers ingested from retail source files.",
    table_properties={"quality": "bronze"},
)
def bronze_orders():
    return read_cloud_files("orders")


@dlt.table(
    name="bronze_order_items",
    comment="Raw order line items ingested from retail source files.",
    table_properties={"quality": "bronze"},
)
def bronze_order_items():
    return read_cloud_files("order_items")


@dlt.table(
    name="bronze_inventory",
    comment="Raw store inventory snapshots ingested from retail source files.",
    table_properties={"quality": "bronze"},
)
def bronze_inventory():
    return read_cloud_files("inventory")


@dlt.table(
    name="silver_customers",
    comment="Validated and standardized customer dimension records.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_email_when_present", "email IS NULL OR email RLIKE '^[^@]+@[^@]+\\.[^@]+$'")
def silver_customers():
    return (
        dlt.read_stream("bronze_customers")
        .select(
            F.col("customer_id").cast(StringType()).alias("customer_id"),
            F.trim(F.col("first_name")).alias("first_name"),
            F.trim(F.col("last_name")).alias("last_name"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.upper(F.trim(F.col("loyalty_tier"))).alias("loyalty_tier"),
            F.to_date("signup_date").alias("signup_date"),
            F.col("_source_file"),
            F.col("_ingest_ts"),
        )
        .withWatermark("_ingest_ts", "1 day")
        .dropDuplicates(["customer_id"])
    )


@dlt.table(
    name="silver_products",
    comment="Validated and standardized product dimension records.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("non_negative_unit_price", "unit_price >= 0")
def silver_products():
    return (
        dlt.read_stream("bronze_products")
        .select(
            F.col("product_id").cast(StringType()).alias("product_id"),
            F.trim(F.col("product_name")).alias("product_name"),
            F.trim(F.col("category")).alias("category"),
            F.trim(F.col("brand")).alias("brand"),
            F.col("unit_price").cast(DecimalType(12, 2)).alias("unit_price"),
            F.col("is_active").cast("boolean").alias("is_active"),
            F.col("_source_file"),
            F.col("_ingest_ts"),
        )
        .fillna({"is_active": True})
        .withWatermark("_ingest_ts", "1 day")
        .dropDuplicates(["product_id"])
    )


@dlt.table(
    name="silver_stores",
    comment="Validated and standardized store dimension records.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
def silver_stores():
    return (
        dlt.read_stream("bronze_stores")
        .select(
            F.col("store_id").cast(StringType()).alias("store_id"),
            F.trim(F.col("store_name")).alias("store_name"),
            F.trim(F.col("city")).alias("city"),
            F.trim(F.col("state")).alias("state"),
            F.trim(F.col("country")).alias("country"),
            F.col("opened_date").cast(DateType()).alias("opened_date"),
            F.col("_source_file"),
            F.col("_ingest_ts"),
        )
        .withWatermark("_ingest_ts", "1 day")
        .dropDuplicates(["store_id"])
    )


@dlt.table(
    name="silver_orders",
    comment="Validated and standardized order header records.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_ts", "order_ts IS NOT NULL")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        .select(
            F.col("order_id").cast(StringType()).alias("order_id"),
            F.col("customer_id").cast(StringType()).alias("customer_id"),
            F.col("store_id").cast(StringType()).alias("store_id"),
            F.col("order_ts").cast(TimestampType()).alias("order_ts"),
            F.upper(F.trim(F.col("order_status"))).alias("order_status"),
            F.upper(F.trim(F.col("payment_method"))).alias("payment_method"),
            F.col("_source_file"),
            F.col("_ingest_ts"),
        )
        .withColumn("order_date", F.to_date("order_ts"))
        .withWatermark("order_ts", "7 days")
        .dropDuplicates(["order_id"])
    )


@dlt.table(
    name="silver_order_items",
    comment="Validated and standardized order line item records.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_order_item_id", "order_item_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("positive_quantity", "quantity > 0")
@dlt.expect_or_drop("non_negative_unit_price", "unit_price >= 0")
def silver_order_items():
    return (
        dlt.read_stream("bronze_order_items")
        .select(
            F.col("order_item_id").cast(StringType()).alias("order_item_id"),
            F.col("order_id").cast(StringType()).alias("order_id"),
            F.col("product_id").cast(StringType()).alias("product_id"),
            F.col("quantity").cast(IntegerType()).alias("quantity"),
            F.col("unit_price").cast(DecimalType(12, 2)).alias("unit_price"),
            F.col("discount_amount").cast(DecimalType(12, 2)).alias("discount_amount"),
            F.col("_source_file"),
            F.col("_ingest_ts"),
        )
        .fillna({"discount_amount": 0})
        .withColumn(
            "line_total",
            (F.col("quantity") * F.col("unit_price")) - F.col("discount_amount"),
        )
        .withWatermark("_ingest_ts", "1 day")
        .dropDuplicates(["order_item_id"])
    )


@dlt.table(
    name="silver_inventory",
    comment="Validated inventory snapshots by store and product.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_snapshot_date", "snapshot_date IS NOT NULL")
@dlt.expect_or_drop("non_negative_on_hand", "quantity_on_hand >= 0")
def silver_inventory():
    return (
        dlt.read_stream("bronze_inventory")
        .select(
            F.col("store_id").cast(StringType()).alias("store_id"),
            F.col("product_id").cast(StringType()).alias("product_id"),
            F.to_date("snapshot_date").alias("snapshot_date"),
            F.col("quantity_on_hand").cast(IntegerType()).alias("quantity_on_hand"),
            F.col("_source_file"),
            F.col("_ingest_ts"),
        )
        .withWatermark("_ingest_ts", "1 day")
        .dropDuplicates(["store_id", "product_id", "snapshot_date"])
    )


@dlt.table(
    name="gold_dim_customer",
    comment="Current customer dimension for reporting.",
    table_properties={"quality": "gold"},
)
def gold_dim_customer():
    return dlt.read("silver_customers").select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "loyalty_tier",
        "signup_date",
    )


@dlt.table(
    name="gold_dim_product",
    comment="Current product dimension for reporting.",
    table_properties={"quality": "gold"},
)
def gold_dim_product():
    return dlt.read("silver_products").select(
        "product_id",
        "product_name",
        "category",
        "brand",
        "unit_price",
        "is_active",
    )


@dlt.table(
    name="gold_dim_store",
    comment="Current store dimension for reporting.",
    table_properties={"quality": "gold"},
)
def gold_dim_store():
    return dlt.read("silver_stores").select(
        "store_id",
        "store_name",
        "city",
        "state",
        "country",
        "opened_date",
    )


@dlt.table(
    name="gold_fact_sales",
    comment="Order item grain sales fact enriched with order, customer, product, and store attributes.",
    table_properties={"quality": "gold"},
)
def gold_fact_sales():
    orders = dlt.read("silver_orders").where("order_status NOT IN ('CANCELLED', 'RETURNED')").alias("o")
    items = dlt.read("silver_order_items").alias("i")
    customers = dlt.read("silver_customers").alias("c")
    products = dlt.read("silver_products").alias("p")
    stores = dlt.read("silver_stores").alias("s")

    return (
        items.join(orders, F.col("i.order_id") == F.col("o.order_id"), "inner")
        .join(customers, F.col("o.customer_id") == F.col("c.customer_id"), "left")
        .join(products, F.col("i.product_id") == F.col("p.product_id"), "left")
        .join(stores, F.col("o.store_id") == F.col("s.store_id"), "left")
        .select(
            F.col("i.order_item_id"),
            F.col("i.order_id"),
            F.col("o.order_date"),
            F.col("o.order_ts"),
            F.col("o.customer_id"),
            F.col("c.loyalty_tier"),
            F.col("o.store_id"),
            F.col("s.store_name"),
            F.col("s.city"),
            F.col("s.state"),
            F.col("i.product_id"),
            F.col("p.product_name"),
            F.col("p.category"),
            F.col("p.brand"),
            F.col("i.quantity"),
            F.col("i.unit_price").alias("sold_unit_price"),
            F.col("i.discount_amount"),
            F.col("i.line_total"),
            F.col("o.payment_method"),
        )
    )


@dlt.table(
    name="gold_daily_sales_summary",
    comment="Daily retail revenue, order, item, and customer metrics by store and category.",
    table_properties={"quality": "gold"},
)
def gold_daily_sales_summary():
    return (
        dlt.read("gold_fact_sales")
        .groupBy("order_date", "store_id", "store_name", "category")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("customer_id").alias("customer_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum("discount_amount").alias("discount_amount"),
            F.sum("line_total").alias("gross_revenue"),
            F.avg("line_total").alias("avg_line_total"),
        )
    )


@dlt.table(
    name="gold_customer_lifetime_value",
    comment="Customer-level purchase totals and recency metrics.",
    table_properties={"quality": "gold"},
)
def gold_customer_lifetime_value():
    return (
        dlt.read("gold_fact_sales")
        .groupBy("customer_id", "loyalty_tier")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("units_purchased"),
            F.sum("line_total").alias("lifetime_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
    )


@dlt.table(
    name="gold_product_performance",
    comment="Product-level demand and revenue metrics.",
    table_properties={"quality": "gold"},
)
def gold_product_performance():
    return (
        dlt.read("gold_fact_sales")
        .groupBy("product_id", "product_name", "category", "brand")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum("line_total").alias("gross_revenue"),
            F.avg("sold_unit_price").alias("avg_sold_unit_price"),
        )
    )

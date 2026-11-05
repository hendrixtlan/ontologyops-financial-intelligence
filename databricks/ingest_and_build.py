# Databricks notebook source
# COMMAND ----------
# Parameterized Lakehouse implementation of the local offline model.
# Export data/generated/raw_payment_events.jsonl to a governed Unity Catalog Volume first.
# In the Databricks UI, create widgets or pass notebook job parameters.
from pyspark.sql import functions as F
from pyspark.sql.window import Window
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------
dbutils.widgets.text("input_path", "/Volumes/main/ontologyops/landing/raw_payment_events.jsonl")
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "ontologyops")
input_path = dbutils.widgets.get("input_path")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
import re
for name in (catalog, schema):
    assert re.fullmatch(r"[a-zA-Z_][a-zA-Z_0-9]*", name), "Unsafe SQL identifier"
namespace = f"{catalog}.{schema}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")

# COMMAND ----------
incoming = spark.read.json(input_path)
required = ["event_id", "transaction_id", "merchant_id", "merchant_tier", "acquirer_id",
            "corridor", "currency", "amount_minor", "status", "event_sequence", "updated_at", "ingested_at"]
assert set(required).issubset(incoming.columns), "Missing event columns"
incoming = incoming.select(*required)
incoming = incoming.withColumn("amount_minor", F.col("amount_minor").cast("long"))
incoming = incoming.withColumn("event_sequence", F.col("event_sequence").cast("int"))
assert incoming.filter((F.col("amount_minor") <= 0) | F.col("amount_minor").isNull()).limit(1).count() == 0
# Idempotent reference snapshot: overwrite deterministically rather than APPEND duplicates.
# For true streaming use Auto Loader + checkpoint + Delta MERGE/CDC semantics.
bronze = incoming.dropDuplicates(["event_id"])
bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{namespace}.raw_payment_events")

# COMMAND ----------
# Order on the event's business timestamp and sequence, not processing arrival.
rank = Window.partitionBy("transaction_id").orderBy(
    F.to_timestamp("updated_at").desc(),
    F.col("event_sequence").desc(),
    F.to_timestamp("ingested_at").desc(),
    F.col("event_id").desc(),
)
silver = (spark.table(f"{namespace}.raw_payment_events")
          .withColumn("_rn", F.row_number().over(rank))
          .filter(F.col("_rn") == 1).drop("_rn"))
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{namespace}.silver_transactions")

# COMMAND ----------
gold = silver.select(
    "transaction_id", F.to_date(F.to_timestamp("updated_at")).alias("business_date"),
    "merchant_id", "merchant_tier", "acquirer_id", "corridor", "currency",
    "amount_minor", "status"
)
gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{namespace}.gold_payment_outcomes")
print({"bronze": bronze.count(), "silver": silver.count(), "gold": gold.count()})

# COMMAND ----------
# Apply the rendered DDL from: ontologyops render-sql
# Then create/publish a Unity Catalog Page from semantics/pages/settlement_business_glossary.md.

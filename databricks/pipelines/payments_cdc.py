"""Databricks Lakeflow Spark Declarative Pipelines (workspace execution only).

Input: managed Kafka endpoint reachable FROM Azure Databricks, receiving
Debezium JSON payloads from public.payment_events. Local Redpanda localhost
is NOT accessible from remote Databricks compute without a network bridge.

Produces:
  bronze_payment_cdc (append-only Kafka envelopes)
  cdc_validated (validated streaming changes)
  cdc_quarantine (malformed or disallowed changes)
  silver_transactions (AUTO CDC type 1 current state per transaction)
  gold_payment_outcomes (materialized latest state; certified metric source)
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                               IntegerType)


payment_schema = StructType([
    StructField("event_id", StringType()),
    StructField("transaction_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_tier", StringType()),
    StructField("acquirer_id", StringType()),
    StructField("corridor", StringType()),
    StructField("currency", StringType()),
    StructField("amount_minor", LongType()),
    StructField("status", StringType()),
    StructField("event_sequence", IntegerType()),
    StructField("updated_at", StringType()),
    StructField("ingested_at", StringType()),
])
envelope_schema = StructType([
    StructField("op", StringType()),
    StructField("after", payment_schema),
])


def kafka_reader():
    """Credentials supplied via workspace-managed Spark configuration, never Git."""
    broker = spark.conf.get("ontologyops.kafka.bootstrap_servers")
    topic = spark.conf.get("ontologyops.kafka.topic")
    protocol = spark.conf.get("ontologyops.kafka.security_protocol", "SASL_SSL")
    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", broker)
              .option("subscribe", topic)
              .option("startingOffsets", "earliest")
              .option("failOnDataLoss", "true")
              .option("kafka.security.protocol", protocol))
    # Local/production auth settings are different. Managed production brokers
    # should use SASL_SSL, with actual secrets configured in the workspace.
    if protocol == "SASL_SSL":
        reader = (reader
                  .option("kafka.sasl.mechanism", spark.conf.get("ontologyops.kafka.sasl_mechanism", "PLAIN"))
                  .option("kafka.sasl.jaas.config", spark.conf.get("ontologyops.kafka.sasl_jaas_config")))
    return reader.load()


@dp.table(name="bronze_payment_cdc", comment="Immutable Kafka records incl. topic/partition/offset for forensic replay")
def bronze_payment_cdc():
    return kafka_reader().select(
        "topic", "partition", "offset", F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_cdc_json"),
    )


@dp.temporary_view(name="parsed_payment_cdc")
def parsed_payment_cdc():
    return (spark.readStream.table("bronze_payment_cdc")
            .withColumn("cdc", F.from_json("raw_cdc_json", envelope_schema))
            .select("topic", "partition", "offset", "kafka_timestamp", "raw_cdc_json",
                    F.col("cdc.op").alias("cdc_op"), "cdc.after.*"))


def is_valid():
    return ((F.col("cdc_op").isin("c", "r", "u"))
            & F.col("event_id").isNotNull()
            & F.col("transaction_id").isNotNull()
            & F.col("merchant_id").isNotNull()
            & F.col("amount_minor").isNotNull()
            & (F.col("amount_minor") > 0)
            & (F.col("event_sequence") > 0)
            & F.col("currency").isin("USD", "MXN")
            & F.col("status").isin("INITIATED", "AUTHORIZED", "PENDING", "FAILED", "SETTLED")
            & F.to_timestamp("updated_at").isNotNull()
            & F.to_timestamp("ingested_at").isNotNull())


@dp.table(name="cdc_validated", comment="Only valid INSERT/read/UPDATE Debezium events")
@dp.expect_or_drop("certified_payment_cdc", "transaction_id IS NOT NULL AND amount_minor > 0")
def cdc_validated():
    return (spark.readStream.table("parsed_payment_cdc")
            .where(is_valid())
            .select("event_id", "transaction_id", "merchant_id", "merchant_tier",
                    "acquirer_id", "corridor", "currency", "amount_minor", "status",
                    "event_sequence", F.to_timestamp("updated_at").alias("updated_at"),
                    F.to_timestamp("ingested_at").alias("ingested_at")))


@dp.table(name="cdc_quarantine", comment="Rejected CDC records incl deletes and malformed source JSON")
def cdc_quarantine():
    return (spark.readStream.table("parsed_payment_cdc")
            .where(~is_valid())
            .select("topic", "partition", "offset", "kafka_timestamp", "raw_cdc_json", "cdc_op"))


# Lakeflow AUTO CDC compares transaction updates in business event order,
# independent of Kafka arrival ordering, and retains latest state for each key.
dp.create_streaming_table(name="silver_transactions", comment="One latest payment-state row per transaction")
dp.create_auto_cdc_flow(
    target="silver_transactions",
    source="cdc_validated",
    keys=["transaction_id"],
    sequence_by=F.struct("updated_at", "event_sequence", "ingested_at", "event_id"),
    stored_as_scd_type=1,
)


@dp.materialized_view(name="gold_payment_outcomes", comment="Latest payment outcomes for certified metric view")
def gold_payment_outcomes():
    return (spark.table("silver_transactions")
            .select("transaction_id", F.to_date("updated_at").alias("business_date"),
                    "merchant_id", "merchant_tier", "acquirer_id", "corridor", "currency",
                    "amount_minor", "status"))

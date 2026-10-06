"""Spark session factory with Delta Lake enabled.

One place decides how Spark is configured, so every job (Bronze, Silver, Gold, tests) behaves
the same. Cluster-specific settings (S3 access, Databricks) are added here later, not scattered
through the transformation code.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from edp.common.config import Settings, get_settings, resolve_path

# Settings that make Spark fast for SMALL data on one machine. They are measured, not guessed:
# on the dev container they cut Delta write/read times by 3-5x. They would be wrong for a real
# cluster (adaptive execution and codegen pay off at scale), so they apply to local masters only.
LOCAL_SMALL_DATA_CONF = {
    "spark.driver.extraJavaOptions": (
        "-XX:+UseSerialGC -XX:TieredStopAtLevel=1 -Djava.security.egd=file:/dev/urandom"
    ),
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.codegen.wholeStage": "false",
    "spark.default.parallelism": "2",
}


def build_spark_session(
    settings: Settings | None = None,
    *,
    app_name: str = "edp",
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """Create (or reuse) a local Spark session with the Delta extensions."""
    settings = settings or get_settings()
    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        # UTC everywhere: timestamps must not depend on the machine's time zone.
        .config("spark.sql.session.timeZone", "UTC")
        # Sized for a laptop: small data, so few shuffle partitions and no web UI.
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        # Avoid hostname-resolution problems inside containers.
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", str(resolve_path("data/spark-warehouse")))
    )
    if settings.spark_master.startswith("local"):
        for key, value in LOCAL_SMALL_DATA_CONF.items():
            builder = builder.config(key, value)
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

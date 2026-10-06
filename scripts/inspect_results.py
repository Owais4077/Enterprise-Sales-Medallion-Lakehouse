"""Quick inspection script to print summary statistics and sample rows from Gold and Quarantine Delta tables."""

from delta.tables import DeltaTable

from edp.common.config import get_settings
from edp.common.paths import LakeLayout, Layer
from edp.common.spark import build_spark_session


def main():
    settings = get_settings()
    spark = build_spark_session(settings, app_name="inspect-results")
    layout = LakeLayout(settings)

    print("\n" + "=" * 80)
    print(" 🌟 GOLD FACT SALES (Top 5 Records with USD Currency Conversion)")
    print("=" * 80)
    fact_path = layout.table_path(Layer.GOLD, "fact_sales")
    df_facts = spark.read.format("delta").load(fact_path)
    df_facts.select(
        "sales_fact_key",
        "order_id",
        "order_date_key",
        "shipping_country",
        "currency",
        "quantity",
        "unit_price_local",
        "line_total_local",
        "exchange_rate_to_usd",
        "line_total_usd",
    ).show(5, truncate=False)

    print("\n" + "=" * 80)
    print(" 📈 GOLD MONTHLY KPI SALES SUMMARY (Top 5 Records)")
    print("=" * 80)
    kpi_path = layout.table_path(Layer.GOLD, "kpi_monthly_sales")
    df_kpi = spark.read.format("delta").load(kpi_path)
    df_kpi.show(5, truncate=False)

    print("\n" + "=" * 80)
    print(" 🛡️ SILVER QUARANTINE REJECTED ORDERS (Sample Rows with Failure Reasons)")
    print("=" * 80)
    q_orders_path = f"{layout.table_path(Layer.SILVER, '_quarantine')}/orders"
    if DeltaTable.isDeltaTable(spark, q_orders_path):
        df_q = spark.read.format("delta").load(q_orders_path)
        df_q.select("order_id", "total_amount", "status", "quarantine_reason").show(
            5, truncate=False
        )
    else:
        print("No quarantine orders found.")

    spark.stop()


if __name__ == "__main__":
    main()

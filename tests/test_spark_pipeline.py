import sys

import pytest

from src import spark_pipeline


class FakeFunctions:
    @staticmethod
    def expr(value):
        return ("expr", value)


class FakeDataFrame:
    def __init__(self, calls=None):
        self.calls = calls or []

    def where(self, expression):
        return FakeDataFrame([*self.calls, ("where", expression)])

    def selectExpr(self, *expressions):
        return FakeDataFrame([*self.calls, ("selectExpr", expressions)])

    def withColumn(self, name, expression):
        return FakeDataFrame([*self.calls, ("withColumn", name, expression)])

    def select(self, *columns):
        return FakeDataFrame([*self.calls, ("select", columns)])


def test_silver_selection_sql_escapes_status_literals_and_date_window():
    expression = spark_pipeline.build_silver_selection_sql(
        ["delivered", "customer's_pickup"],
        order_date_start="2026-06-01",
        order_date_end="2026-06-30",
    )

    assert expression == (
        "status in ('delivered', 'customer''s_pickup') and "
        "order_date >= '2026-06-01' and order_date <= '2026-06-30'"
    )


def test_silver_selection_sql_rejects_empty_status_scope():
    with pytest.raises(ValueError, match="included_statuses"):
        spark_pipeline.build_silver_selection_sql([])


def test_spark_transform_uses_existing_silver_and_rejected_contracts(monkeypatch):
    monkeypatch.setattr(
        spark_pipeline,
        "_require_pyspark_functions",
        lambda: FakeFunctions,
    )

    silver_df, rejected_df = spark_pipeline.build_silver_and_rejected_dataframes(
        FakeDataFrame(),
        ["delivered"],
        order_date_start="2026-06-01",
        order_date_end="2026-06-30",
    )

    selection_sql = (
        "status in ('delivered') and order_date >= '2026-06-01' "
        "and order_date <= '2026-06-30'"
    )
    assert silver_df.calls == [
        ("where", selection_sql),
        (
            "selectExpr",
            (
                "order_id",
                "customer_id",
                "order_date",
                "category",
                "product",
                "cast(quantity as int) as quantity",
                "cast(unit_price as double) as unit_price",
                "cast(quantity as int) * cast(unit_price as double) as revenue",
            ),
        ),
    ]
    assert rejected_df.calls == [
        ("where", f"not ({selection_sql})"),
        (
            "withColumn",
            "rejection_reason",
            (
                "expr",
                "case when status not in ('delivered') then "
                "'status_not_included' when order_date < '2026-06-01' then "
                "'order_date_out_of_range' when order_date > '2026-06-30' then "
                "'order_date_out_of_range' end",
            ),
        ),
        ("select", tuple(spark_pipeline.REJECTED_COLUMNS)),
    ]


def test_spark_pipeline_reports_missing_optional_dependency(monkeypatch):
    monkeypatch.setitem(sys.modules, "pyspark", None)
    monkeypatch.setitem(sys.modules, "pyspark.sql", None)

    with pytest.raises(RuntimeError, match="PySpark is required"):
        spark_pipeline._require_pyspark()

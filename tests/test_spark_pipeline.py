import json
import sys

import pytest

from src import spark_pipeline


class FakeFunctions:
    @staticmethod
    def expr(value):
        return ("expr", value)


class FakeDataFrame:
    def __init__(self, calls=None, count_value=0, columns=None):
        self.calls = calls or []
        self.count_value = count_value
        self.columns = columns or []

    def where(self, expression):
        return FakeDataFrame([*self.calls, ("where", expression)], self.count_value)

    def selectExpr(self, *expressions):
        return FakeDataFrame(
            [*self.calls, ("selectExpr", expressions)],
            self.count_value,
        )

    def withColumn(self, name, expression):
        return FakeDataFrame(
            [*self.calls, ("withColumn", name, expression)],
            self.count_value,
        )

    def select(self, *columns):
        return FakeDataFrame([*self.calls, ("select", columns)], self.count_value)

    def count(self):
        return self.count_value


class FakeDataFrameWriter:
    def __init__(self, written_paths):
        self.written_paths = written_paths

    def mode(self, mode):
        self.mode_value = mode
        return self

    def parquet(self, path):
        self.written_paths.append((self.mode_value, path))


class FakeWritableDataFrame(FakeDataFrame):
    def __init__(self, count_value, written_paths, columns=None):
        super().__init__(count_value=count_value, columns=columns)
        self.write = FakeDataFrameWriter(written_paths)


class FakeSparkReader:
    def __init__(self, raw_df):
        self.raw_df = raw_df
        self.options = []

    def option(self, key, value):
        self.options.append((key, value))
        return self

    def csv(self, path):
        self.csv_path = path
        return self.raw_df


class FakeSparkSession:
    reader = None
    app_name = None
    stop_count = 0

    def __init__(self):
        self.read = self.reader

    def stop(self):
        type(self).stop_count += 1


class FakeSparkSessionBuilder:
    def appName(self, name):
        FakeSparkSession.app_name = name
        return self

    def getOrCreate(self):
        return FakeSparkSession()


FakeSparkSession.builder = FakeSparkSessionBuilder()


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


def test_spark_row_count_reconciliation_reports_accounted_rows():
    reconciliation = spark_pipeline.build_spark_row_count_reconciliation(
        FakeDataFrame(count_value=3),
        FakeDataFrame(count_value=2),
        FakeDataFrame(count_value=1),
    )

    assert reconciliation == {
        "success": True,
        "bronze_rows": 3,
        "silver_rows": 2,
        "rejected_rows": 1,
        "accounted_rows": 3,
        "difference": 0,
    }


def test_spark_output_contract_validation_detects_column_drift():
    validation = spark_pipeline.build_spark_output_contract_validation(
        {
            "silver_orders": {
                "dataframe": FakeDataFrame(
                    columns=[
                        "order_id",
                        "customer_id",
                        "order_date",
                        "category",
                        "product",
                        "quantity",
                        "revenue",
                    ],
                ),
                "expected_columns": spark_pipeline.SILVER_COLUMNS,
            },
            "rejected_orders": {
                "dataframe": FakeDataFrame(columns=spark_pipeline.REJECTED_COLUMNS),
                "expected_columns": spark_pipeline.REJECTED_COLUMNS,
            },
        }
    )

    assert validation["success"] is False
    assert validation["failed_outputs"] == ["silver_orders"]
    assert validation["outputs"]["silver_orders"]["missing_columns"] == [
        "unit_price"
    ]
    assert validation["outputs"]["silver_orders"]["order_matches"] is False
    with pytest.raises(
        ValueError,
        match="Spark output contract validation failed: silver_orders",
    ):
        spark_pipeline.raise_for_failed_spark_output_contract_validation(validation)


def test_spark_pipeline_reconciles_counts_before_writing(tmp_path, monkeypatch):
    raw_path = tmp_path / "raw" / "orders.csv"
    processed_dir = tmp_path / "processed"
    config_path = tmp_path / "pipeline.json"
    raw_path.parent.mkdir()
    raw_path.write_text(
        "order_id,customer_id,order_date,category,product,quantity,unit_price,status\n",
        encoding="utf-8",
    )
    config_path.write_text(
        json.dumps(
            {
                "raw_path": str(raw_path),
                "processed_dir": str(processed_dir),
                "included_statuses": ["delivered"],
            }
        ),
        encoding="utf-8",
    )
    written_paths = []
    raw_df = FakeDataFrame(count_value=3)
    silver_df = FakeWritableDataFrame(
        2,
        written_paths,
        columns=spark_pipeline.SILVER_COLUMNS,
    )
    rejected_df = FakeWritableDataFrame(
        1,
        written_paths,
        columns=spark_pipeline.REJECTED_COLUMNS,
    )
    FakeSparkSession.reader = FakeSparkReader(raw_df)
    FakeSparkSession.app_name = None
    FakeSparkSession.stop_count = 0

    monkeypatch.setattr(spark_pipeline, "_require_pyspark", lambda: FakeSparkSession)
    monkeypatch.setattr(
        spark_pipeline,
        "build_silver_and_rejected_dataframes",
        lambda *args, **kwargs: (silver_df, rejected_df),
    )

    result = spark_pipeline.run_spark_silver_pipeline(config_path)

    assert FakeSparkSession.app_name == "retail-lakehouse-silver"
    assert FakeSparkSession.reader.options == [("header", True)]
    assert FakeSparkSession.reader.csv_path == str(raw_path)
    assert result == {
        "silver_path": str(processed_dir / "spark_silver_orders"),
        "rejected_path": str(processed_dir / "spark_rejected_orders"),
        "manifest_path": str(processed_dir / "spark_pipeline_manifest.json"),
        "reconciliation": {
            "success": True,
            "bronze_rows": 3,
            "silver_rows": 2,
            "rejected_rows": 1,
            "accounted_rows": 3,
            "difference": 0,
        },
    }
    assert written_paths == [
        ("overwrite", str(processed_dir / "spark_silver_orders")),
        ("overwrite", str(processed_dir / "spark_rejected_orders")),
    ]
    assert FakeSparkSession.stop_count == 1

    manifest = json.loads(
        (processed_dir / "spark_pipeline_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["version"] == 1
    assert manifest["engine"] == "spark"
    assert manifest["run"]["config_path"] == str(config_path.resolve())
    assert manifest["run"]["raw_path"] == str(raw_path)
    assert manifest["run"]["processed_dir"] == str(processed_dir)
    assert manifest["run"]["started_at_utc"].endswith("Z")
    assert manifest["run"]["completed_at_utc"].endswith("Z")
    assert manifest["run"]["duration_ms"] >= 0
    assert manifest["runtime_environment"]["version"] == 1
    assert manifest["source"]["path"] == str(raw_path)
    assert manifest["source"]["rows"] == 3
    assert manifest["config"] == {
        "included_statuses": ["delivered"],
        "order_date_window": {"start": None, "end": None},
    }
    assert manifest["outputs"] == {
        "silver_orders": {
            "path": str(processed_dir / "spark_silver_orders"),
            "format": "parquet",
            "columns": spark_pipeline.SILVER_COLUMNS,
            "rows": 2,
        },
        "rejected_orders": {
            "path": str(processed_dir / "spark_rejected_orders"),
            "format": "parquet",
            "columns": spark_pipeline.REJECTED_COLUMNS,
            "rows": 1,
        },
    }
    assert manifest["reconciliation"] == result["reconciliation"]
    assert manifest["schema_contract_validation"] == {
        "version": 1,
        "success": True,
        "failed_outputs": [],
        "outputs": {
            "silver_orders": {
                "success": True,
                "expected_columns": spark_pipeline.SILVER_COLUMNS,
                "actual_columns": spark_pipeline.SILVER_COLUMNS,
                "missing_columns": [],
                "unexpected_columns": [],
                "order_matches": True,
            },
            "rejected_orders": {
                "success": True,
                "expected_columns": spark_pipeline.REJECTED_COLUMNS,
                "actual_columns": spark_pipeline.REJECTED_COLUMNS,
                "missing_columns": [],
                "unexpected_columns": [],
                "order_matches": True,
            },
        },
    }


def test_spark_pipeline_fails_before_writing_when_counts_do_not_balance(
    tmp_path,
    monkeypatch,
):
    config_path = tmp_path / "pipeline.json"
    raw_path = tmp_path / "orders.csv"
    processed_dir = tmp_path / "processed"
    raw_path.write_text(
        "order_id,customer_id,order_date,category,product,quantity,unit_price,status\n",
        encoding="utf-8",
    )
    config_path.write_text(
        json.dumps(
            {
                "raw_path": str(raw_path),
                "processed_dir": str(processed_dir),
                "included_statuses": ["delivered"],
            }
        ),
        encoding="utf-8",
    )
    written_paths = []
    raw_df = FakeDataFrame(count_value=3)
    silver_df = FakeWritableDataFrame(
        1,
        written_paths,
        columns=spark_pipeline.SILVER_COLUMNS,
    )
    rejected_df = FakeWritableDataFrame(
        1,
        written_paths,
        columns=spark_pipeline.REJECTED_COLUMNS,
    )
    FakeSparkSession.reader = FakeSparkReader(raw_df)
    FakeSparkSession.stop_count = 0

    monkeypatch.setattr(spark_pipeline, "_require_pyspark", lambda: FakeSparkSession)
    monkeypatch.setattr(
        spark_pipeline,
        "build_silver_and_rejected_dataframes",
        lambda *args, **kwargs: (silver_df, rejected_df),
    )

    with pytest.raises(ValueError, match="Row count reconciliation failed"):
        spark_pipeline.run_spark_silver_pipeline(config_path)

    assert written_paths == []
    assert FakeSparkSession.stop_count == 1
    assert not (processed_dir / "spark_pipeline_manifest.json").exists()


def test_spark_pipeline_fails_before_writing_when_output_contract_drifts(
    tmp_path,
    monkeypatch,
):
    config_path = tmp_path / "pipeline.json"
    raw_path = tmp_path / "orders.csv"
    processed_dir = tmp_path / "processed"
    raw_path.write_text(
        "order_id,customer_id,order_date,category,product,quantity,unit_price,status\n",
        encoding="utf-8",
    )
    config_path.write_text(
        json.dumps(
            {
                "raw_path": str(raw_path),
                "processed_dir": str(processed_dir),
                "included_statuses": ["delivered"],
            }
        ),
        encoding="utf-8",
    )
    written_paths = []
    raw_df = FakeDataFrame(count_value=3)
    silver_df = FakeWritableDataFrame(
        2,
        written_paths,
        columns=[
            "order_id",
            "customer_id",
            "order_date",
            "category",
            "product",
            "quantity",
            "revenue",
        ],
    )
    rejected_df = FakeWritableDataFrame(
        1,
        written_paths,
        columns=spark_pipeline.REJECTED_COLUMNS,
    )
    FakeSparkSession.reader = FakeSparkReader(raw_df)
    FakeSparkSession.stop_count = 0

    monkeypatch.setattr(spark_pipeline, "_require_pyspark", lambda: FakeSparkSession)
    monkeypatch.setattr(
        spark_pipeline,
        "build_silver_and_rejected_dataframes",
        lambda *args, **kwargs: (silver_df, rejected_df),
    )

    with pytest.raises(
        ValueError,
        match="Spark output contract validation failed: silver_orders",
    ):
        spark_pipeline.run_spark_silver_pipeline(config_path)

    assert written_paths == []
    assert FakeSparkSession.stop_count == 1
    assert not (processed_dir / "spark_pipeline_manifest.json").exists()


def test_spark_pipeline_reports_missing_optional_dependency(monkeypatch):
    monkeypatch.setitem(sys.modules, "pyspark", None)
    monkeypatch.setitem(sys.modules, "pyspark.sql", None)

    with pytest.raises(RuntimeError, match="PySpark is required"):
        spark_pipeline._require_pyspark()

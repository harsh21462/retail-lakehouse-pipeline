import json
from pathlib import Path
import sys

import pytest

from src import spark_pipeline


class FakeFunctions:
    @staticmethod
    def expr(value):
        return ("expr", value)


class FakeDataFrame:
    def __init__(
        self,
        calls=None,
        count_value=0,
        columns=None,
        aggregate_row=None,
    ):
        self.calls = calls or []
        self.count_value = count_value
        self.columns = columns or []
        self.aggregate_row = aggregate_row
        self.temp_view_name = None

    def where(self, expression):
        return FakeDataFrame(
            [*self.calls, ("where", expression)],
            self.count_value,
            aggregate_row=self.aggregate_row,
        )

    def selectExpr(self, *expressions):
        return FakeDataFrame(
            [*self.calls, ("selectExpr", expressions)],
            self.count_value,
            aggregate_row=self.aggregate_row,
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

    def collect(self):
        if self.aggregate_row is None:
            return []
        return [self.aggregate_row]

    def createOrReplaceTempView(self, name):
        self.temp_view_name = name


class FakeDataFrameWriter:
    def __init__(self, written_paths):
        self.written_paths = written_paths

    def mode(self, mode):
        self.mode_value = mode
        return self

    def parquet(self, path):
        self.written_paths.append((self.mode_value, path))
        output_dir = Path(path)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "part-00000.parquet").write_text(
            f"{output_dir.name}\n",
            encoding="utf-8",
        )


class FailingDataFrameWriter(FakeDataFrameWriter):
    def parquet(self, path):
        self.written_paths.append((self.mode_value, path))
        raise OSError("simulated spark write failure")


class FakeWritableDataFrame(FakeDataFrame):
    def __init__(self, count_value, written_paths, columns=None, aggregate_row=None):
        super().__init__(
            count_value=count_value,
            columns=columns,
            aggregate_row=aggregate_row,
        )
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
    sql_queries = []
    written_paths = []

    def __init__(self):
        self.read = self.reader

    def sql(self, query):
        type(self).sql_queries.append(query)
        if "from rejected_orders" in query:
            columns = spark_pipeline.GOLD_REJECTION_FIELDS
            aggregate_row = {"orders": 1, "units": 1, "revenue": 800.0}
        elif "group by customer_id" in query:
            columns = spark_pipeline.GOLD_CUSTOMER_FIELDS
            aggregate_row = {"orders": 2, "units": 4, "revenue": 8000.0}
        elif "group by category" in query:
            columns = spark_pipeline.GOLD_CATEGORY_FIELDS
            aggregate_row = {"orders": 2, "units": 4, "revenue": 8000.0}
        else:
            columns = spark_pipeline.GOLD_REVENUE_FIELDS
            aggregate_row = {"orders": 2, "units": 4, "revenue": 8000.0}
        return FakeWritableDataFrame(
            1,
            type(self).written_paths,
            columns=columns,
            aggregate_row=aggregate_row,
        )

    def stop(self):
        type(self).stop_count += 1


class FakeSparkSessionBuilder:
    def appName(self, name):
        FakeSparkSession.app_name = name
        return self

    def getOrCreate(self):
        return FakeSparkSession()


FakeSparkSession.builder = FakeSparkSessionBuilder()


def test_spark_cli_uses_default_config_path_when_not_overridden():
    args = spark_pipeline.parse_args([])

    assert args.config == spark_pipeline.DEFAULT_CONFIG_PATH


def test_spark_cli_accepts_config_path_override(tmp_path, monkeypatch):
    config_path = tmp_path / "pipeline.json"
    called_with = []

    def fake_run_spark_silver_pipeline(path):
        called_with.append(path)
        return {"manifest_path": "manifest.json"}

    monkeypatch.setattr(
        spark_pipeline,
        "run_spark_silver_pipeline",
        fake_run_spark_silver_pipeline,
    )

    result = spark_pipeline.cli(["--config", str(config_path)])

    assert called_with == [config_path]
    assert result == {"manifest_path": "manifest.json"}


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


def test_spark_health_warnings_match_pipeline_threshold_shape():
    warnings = spark_pipeline.build_spark_health_warnings(
        bronze_count=4,
        silver_count=1,
        rejected_count=3,
        latest_order_date=spark_pipeline._parse_order_date("2026-08-01"),
        warning_thresholds={
            "max_rejection_rate": 0.5,
            "min_silver_rows": 2,
            "max_source_lag_days": 7,
        },
        as_of_date=spark_pipeline._parse_order_date("2026-08-15"),
    )

    assert warnings == [
        {
            "name": "rejection_rate_above_threshold",
            "severity": "warning",
            "message": "Rejected row rate exceeded configured warning threshold",
            "observed": {
                "bronze_rows": 4,
                "rejected_rows": 3,
                "rejection_rate": 0.75,
            },
            "threshold": {"max_rejection_rate": 0.5},
        },
        {
            "name": "silver_rows_below_threshold",
            "severity": "warning",
            "message": "Silver row count fell below configured warning threshold",
            "observed": {"silver_rows": 1},
            "threshold": {"min_silver_rows": 2},
        },
        {
            "name": "source_lag_above_threshold",
            "severity": "warning",
            "message": (
                "Latest source order date is older than configured "
                "freshness threshold"
            ),
            "observed": {
                "latest_order_date": "2026-08-01",
                "as_of_date": "2026-08-15",
                "source_lag_days": 14,
            },
            "threshold": {"max_source_lag_days": 7},
        },
    ]


def test_spark_health_warnings_report_future_dated_source_data():
    warnings = spark_pipeline.build_spark_health_warnings(
        bronze_count=2,
        silver_count=2,
        rejected_count=0,
        latest_order_date=spark_pipeline._parse_order_date("2026-08-20"),
        warning_thresholds={"max_future_order_date_days": 2},
        as_of_date=spark_pipeline._parse_order_date("2026-08-15"),
    )

    assert warnings == [
        {
            "name": "future_order_date_above_threshold",
            "severity": "warning",
            "message": (
                "Latest source order date is farther in the future "
                "than configured warning threshold"
            ),
            "observed": {
                "latest_order_date": "2026-08-20",
                "as_of_date": "2026-08-15",
                "future_order_date_days": 5,
            },
            "threshold": {"max_future_order_date_days": 2},
        },
    ]


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


def test_spark_gold_dataframes_run_checked_in_sql_models():
    spark = FakeSparkSession()
    FakeSparkSession.sql_queries = []
    FakeSparkSession.written_paths = []
    silver_df = FakeDataFrame(columns=spark_pipeline.SILVER_COLUMNS)
    rejected_df = FakeDataFrame(columns=spark_pipeline.REJECTED_COLUMNS)

    outputs = spark_pipeline.build_spark_gold_dataframes(
        spark,
        silver_df,
        rejected_df,
    )

    assert silver_df.temp_view_name == "silver_orders"
    assert rejected_df.temp_view_name == "rejected_orders"
    assert list(outputs) == [
        "gold_revenue_metrics",
        "gold_customer_metrics",
        "gold_category_metrics",
        "gold_rejection_metrics",
    ]
    assert outputs["gold_revenue_metrics"]["expected_columns"] == (
        spark_pipeline.GOLD_REVENUE_FIELDS
    )
    assert outputs["gold_customer_metrics"]["expected_columns"] == (
        spark_pipeline.GOLD_CUSTOMER_FIELDS
    )
    assert outputs["gold_category_metrics"]["expected_columns"] == (
        spark_pipeline.GOLD_CATEGORY_FIELDS
    )
    assert outputs["gold_rejection_metrics"]["expected_columns"] == (
        spark_pipeline.GOLD_REJECTION_FIELDS
    )
    assert FakeSparkSession.sql_queries == [
        spark_pipeline._read_spark_sql_model(spark_pipeline.GOLD_SQL_PATH),
        spark_pipeline._read_spark_sql_model(
            spark_pipeline.GOLD_CUSTOMER_SQL_PATH
        ),
        spark_pipeline._read_spark_sql_model(
            spark_pipeline.GOLD_CATEGORY_SQL_PATH
        ),
        spark_pipeline._read_spark_sql_model(
            spark_pipeline.GOLD_REJECTION_SQL_PATH
        ),
    ]


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
        aggregate_row={"orders": 2, "units": 4, "revenue": 8000.0},
    )
    rejected_df = FakeWritableDataFrame(
        1,
        written_paths,
        columns=spark_pipeline.REJECTED_COLUMNS,
        aggregate_row={"orders": 1, "units": 1, "revenue": 800.0},
    )
    FakeSparkSession.reader = FakeSparkReader(raw_df)
    FakeSparkSession.app_name = None
    FakeSparkSession.stop_count = 0
    FakeSparkSession.sql_queries = []
    FakeSparkSession.written_paths = written_paths

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
    assert len(written_paths) == 6
    assert written_paths[0][0] == "overwrite"
    assert written_paths[1][0] == "overwrite"
    assert Path(written_paths[0][1]).name.startswith(".spark_silver_orders.staged.")
    assert Path(written_paths[1][1]).name.startswith(
        ".spark_rejected_orders.staged."
    )
    assert (processed_dir / "spark_silver_orders" / "part-00000.parquet").exists()
    assert (
        processed_dir / "spark_rejected_orders" / "part-00000.parquet"
    ).exists()
    assert (
        processed_dir / "spark_gold_revenue_metrics" / "part-00000.parquet"
    ).exists()
    assert (
        processed_dir / "spark_gold_customer_metrics" / "part-00000.parquet"
    ).exists()
    assert (
        processed_dir / "spark_gold_category_metrics" / "part-00000.parquet"
    ).exists()
    assert (
        processed_dir / "spark_gold_rejection_metrics" / "part-00000.parquet"
    ).exists()
    assert list(processed_dir.glob(".spark_*_orders.staged.*")) == []
    assert FakeSparkSession.stop_count == 1
    assert FakeSparkSession.sql_queries == [
        spark_pipeline._read_spark_sql_model(spark_pipeline.GOLD_SQL_PATH),
        spark_pipeline._read_spark_sql_model(
            spark_pipeline.GOLD_CUSTOMER_SQL_PATH
        ),
        spark_pipeline._read_spark_sql_model(
            spark_pipeline.GOLD_CATEGORY_SQL_PATH
        ),
        spark_pipeline._read_spark_sql_model(
            spark_pipeline.GOLD_REJECTION_SQL_PATH
        ),
    ]

    manifest = json.loads(
        (processed_dir / "spark_pipeline_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["version"] == 1
    assert manifest["engine"] == "spark"
    assert manifest["run"]["config_path"] == str(config_path.resolve())
    assert manifest["run"]["config_file_audit"]["path"] == str(config_path)
    assert manifest["run"]["config_file_audit"]["exists"] is True
    assert manifest["run"]["config_file_audit"]["type"] == "file"
    assert manifest["run"]["config_file_audit"]["bytes"] == config_path.stat().st_size
    assert manifest["run"]["config_file_audit"]["modified_at_utc"].endswith("Z")
    assert manifest["run"]["raw_path"] == str(raw_path)
    assert manifest["run"]["processed_dir"] == str(processed_dir)
    assert manifest["run"]["started_at_utc"].endswith("Z")
    assert manifest["run"]["completed_at_utc"].endswith("Z")
    assert manifest["run"]["duration_ms"] >= 0
    assert manifest["runtime_environment"]["version"] == 1
    assert manifest["source"]["path"] == str(raw_path)
    assert manifest["source"]["file_audit"]["path"] == str(raw_path)
    assert manifest["source"]["file_audit"]["exists"] is True
    assert manifest["source"]["file_audit"]["type"] == "file"
    assert manifest["source"]["file_audit"]["bytes"] == raw_path.stat().st_size
    assert manifest["source"]["file_audit"]["modified_at_utc"].endswith("Z")
    assert manifest["source"]["rows"] == 3
    assert manifest["config"] == {
        "included_statuses": ["delivered"],
        "order_date_window": {"start": None, "end": None},
        "warning_thresholds": {},
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
        "gold_revenue_metrics": {
            "path": str(processed_dir / "spark_gold_revenue_metrics"),
            "format": "parquet",
            "columns": spark_pipeline.GOLD_REVENUE_FIELDS,
            "rows": 1,
        },
        "gold_customer_metrics": {
            "path": str(processed_dir / "spark_gold_customer_metrics"),
            "format": "parquet",
            "columns": spark_pipeline.GOLD_CUSTOMER_FIELDS,
            "rows": 1,
        },
        "gold_category_metrics": {
            "path": str(processed_dir / "spark_gold_category_metrics"),
            "format": "parquet",
            "columns": spark_pipeline.GOLD_CATEGORY_FIELDS,
            "rows": 1,
        },
        "gold_rejection_metrics": {
            "path": str(processed_dir / "spark_gold_rejection_metrics"),
            "format": "parquet",
            "columns": spark_pipeline.GOLD_REJECTION_FIELDS,
            "rows": 1,
        },
    }
    assert set(manifest["output_inventory"]) == set(manifest["outputs"])
    for artifact_name, artifact_stats in manifest["output_inventory"].items():
        assert artifact_stats["path"] == manifest["outputs"][artifact_name]["path"]
        assert artifact_stats["exists"] is True
        assert artifact_stats["type"] == "directory"
        assert artifact_stats["files"] == 1
        assert artifact_stats["bytes"] > 0
        assert artifact_stats["sha256"]
    assert manifest["health"] == {
        "status": "passed",
        "warnings": [],
        "warning_count": 0,
        "threshold_breaches": [],
    }
    assert manifest["reconciliation"] == result["reconciliation"]
    assert manifest["metric_reconciliation"] == {
        "version": 1,
        "success": True,
        "failed_checks": [],
        "checks": [
            {
                "name": "gold_revenue_orders_match_silver",
                "success": True,
                "expected": 2,
                "actual": 2,
                "difference": 0,
            },
            {
                "name": "gold_revenue_units_match_silver",
                "success": True,
                "expected": 4,
                "actual": 4,
                "difference": 0,
            },
            {
                "name": "gold_revenue_amount_match_silver",
                "success": True,
                "expected": 8000.0,
                "actual": 8000.0,
                "difference": 0.0,
            },
            {
                "name": "gold_customer_orders_match_silver",
                "success": True,
                "expected": 2,
                "actual": 2,
                "difference": 0,
            },
            {
                "name": "gold_customer_units_match_silver",
                "success": True,
                "expected": 4,
                "actual": 4,
                "difference": 0,
            },
            {
                "name": "gold_customer_revenue_match_silver",
                "success": True,
                "expected": 8000.0,
                "actual": 8000.0,
                "difference": 0.0,
            },
            {
                "name": "gold_category_orders_match_silver",
                "success": True,
                "expected": 2,
                "actual": 2,
                "difference": 0,
            },
            {
                "name": "gold_category_units_match_silver",
                "success": True,
                "expected": 4,
                "actual": 4,
                "difference": 0,
            },
            {
                "name": "gold_category_revenue_match_silver",
                "success": True,
                "expected": 8000.0,
                "actual": 8000.0,
                "difference": 0.0,
            },
            {
                "name": "gold_rejection_orders_match_rejected",
                "success": True,
                "expected": 1,
                "actual": 1,
                "difference": 0,
            },
            {
                "name": "gold_rejection_units_match_rejected",
                "success": True,
                "expected": 1,
                "actual": 1,
                "difference": 0,
            },
            {
                "name": "gold_rejection_revenue_match_rejected",
                "success": True,
                "expected": 800.0,
                "actual": 800.0,
                "difference": 0.0,
            },
        ],
    }
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
            "gold_revenue_metrics": {
                "success": True,
                "expected_columns": spark_pipeline.GOLD_REVENUE_FIELDS,
                "actual_columns": spark_pipeline.GOLD_REVENUE_FIELDS,
                "missing_columns": [],
                "unexpected_columns": [],
                "order_matches": True,
            },
            "gold_customer_metrics": {
                "success": True,
                "expected_columns": spark_pipeline.GOLD_CUSTOMER_FIELDS,
                "actual_columns": spark_pipeline.GOLD_CUSTOMER_FIELDS,
                "missing_columns": [],
                "unexpected_columns": [],
                "order_matches": True,
            },
            "gold_category_metrics": {
                "success": True,
                "expected_columns": spark_pipeline.GOLD_CATEGORY_FIELDS,
                "actual_columns": spark_pipeline.GOLD_CATEGORY_FIELDS,
                "missing_columns": [],
                "unexpected_columns": [],
                "order_matches": True,
            },
            "gold_rejection_metrics": {
                "success": True,
                "expected_columns": spark_pipeline.GOLD_REJECTION_FIELDS,
                "actual_columns": spark_pipeline.GOLD_REJECTION_FIELDS,
                "missing_columns": [],
                "unexpected_columns": [],
                "order_matches": True,
            },
        },
    }
    assert manifest["sql_models"] == spark_pipeline.build_sql_model_inventory(
        {
            "silver_orders": processed_dir / "spark_silver_orders",
            "rejected_orders": processed_dir / "spark_rejected_orders",
            "gold_revenue_metrics": (
                processed_dir / "spark_gold_revenue_metrics"
            ),
            "gold_customer_metrics": (
                processed_dir / "spark_gold_customer_metrics"
            ),
            "gold_category_metrics": (
                processed_dir / "spark_gold_category_metrics"
            ),
            "gold_rejection_metrics": (
                processed_dir / "spark_gold_rejection_metrics"
            ),
        }
    )
    assert manifest["data_catalog"]["version"] == 1
    assert manifest["data_catalog"]["engine"] == "spark"
    assert manifest["data_catalog"]["format"] == "manifest_embedded"
    assert manifest["data_catalog"]["tables"][0] == {
        "name": "gold_category_metrics",
        "description": "Spark category-level order and revenue metrics.",
        "format": "parquet",
        "path": str(processed_dir / "spark_gold_category_metrics"),
        "rows": 1,
        "columns": [
            {"name": column_name}
            for column_name in spark_pipeline.GOLD_CATEGORY_FIELDS
        ],
    }
    assert {table["name"] for table in manifest["data_catalog"]["tables"]} == set(
        manifest["outputs"]
    )
    assert manifest["lineage"]["version"] == 1
    assert manifest["lineage"]["engine"] == "spark"
    assert manifest["lineage"]["root"] == str(processed_dir)
    assert {
        "from": "source.raw_orders",
        "to": "spark.silver_orders",
    } in manifest["lineage"]["edges"]
    assert {
        "from": "spark.silver_orders",
        "to": "spark.gold_revenue_metrics",
    } in manifest["lineage"]["edges"]
    assert {
        "from": "spark.rejected_orders",
        "to": "spark.gold_rejection_metrics",
    } in manifest["lineage"]["edges"]
    assert {
        "from": "spark.gold_revenue_metrics",
        "to": "catalog.spark_data_catalog",
    } in manifest["lineage"]["edges"]
    assert manifest["run_comparison"] == {
        "version": 1,
        "previous_manifest_available": False,
        "unavailable_reason": "not_found",
    }


def test_spark_data_catalog_embeds_output_contracts(tmp_path):
    output_paths = {
        "silver_orders": tmp_path / "spark_silver_orders",
        "gold_revenue_metrics": tmp_path / "spark_gold_revenue_metrics",
    }
    validation = {
        "outputs": {
            "silver_orders": {
                "expected_columns": spark_pipeline.SILVER_COLUMNS,
            },
            "gold_revenue_metrics": {
                "expected_columns": spark_pipeline.GOLD_REVENUE_FIELDS,
            },
        },
    }

    catalog = spark_pipeline.build_spark_data_catalog(
        output_paths,
        {
            "silver_orders": 42,
            "gold_revenue_metrics": 7,
        },
        validation,
    )

    assert catalog == {
        "version": 1,
        "format": "manifest_embedded",
        "engine": "spark",
        "tables": [
            {
                "name": "gold_revenue_metrics",
                "description": "Spark revenue metrics by order date and category.",
                "format": "parquet",
                "path": str(tmp_path / "spark_gold_revenue_metrics"),
                "rows": 7,
                "columns": [
                    {"name": column_name}
                    for column_name in spark_pipeline.GOLD_REVENUE_FIELDS
                ],
            },
            {
                "name": "silver_orders",
                "description": (
                    "Cleaned analytics-ready orders produced by Spark."
                ),
                "format": "parquet",
                "path": str(tmp_path / "spark_silver_orders"),
                "rows": 42,
                "columns": [
                    {"name": column_name}
                    for column_name in spark_pipeline.SILVER_COLUMNS
                ],
            },
        ],
    }


def test_spark_lineage_links_source_outputs_and_catalog(tmp_path):
    output_paths = {
        "silver_orders": tmp_path / "spark_silver_orders",
        "rejected_orders": tmp_path / "spark_rejected_orders",
        "gold_revenue_metrics": tmp_path / "spark_gold_revenue_metrics",
        "gold_customer_metrics": tmp_path / "spark_gold_customer_metrics",
        "gold_category_metrics": tmp_path / "spark_gold_category_metrics",
        "gold_rejection_metrics": tmp_path / "spark_gold_rejection_metrics",
    }

    lineage = spark_pipeline.build_spark_lineage(
        raw_path=tmp_path / "orders.csv",
        processed_dir=tmp_path,
        output_paths=output_paths,
    )

    assert lineage["version"] == 1
    assert lineage["engine"] == "spark"
    assert lineage["root"] == str(tmp_path)
    assert {node["id"] for node in lineage["nodes"]} == {
        "source.raw_orders",
        "catalog.spark_data_catalog",
        "spark.silver_orders",
        "spark.rejected_orders",
        "spark.gold_revenue_metrics",
        "spark.gold_customer_metrics",
        "spark.gold_category_metrics",
        "spark.gold_rejection_metrics",
    }
    assert {
        "from": "source.raw_orders",
        "to": "spark.silver_orders",
    } in lineage["edges"]
    assert {
        "from": "spark.silver_orders",
        "to": "spark.gold_customer_metrics",
    } in lineage["edges"]
    assert {
        "from": "spark.rejected_orders",
        "to": "spark.gold_rejection_metrics",
    } in lineage["edges"]
    assert {
        "from": "spark.gold_category_metrics",
        "to": "catalog.spark_data_catalog",
    } in lineage["edges"]


def test_spark_metric_reconciliation_detects_gold_aggregate_drift():
    written_paths = []
    silver_df = FakeWritableDataFrame(
        1,
        written_paths,
        columns=spark_pipeline.SILVER_COLUMNS,
        aggregate_row={"orders": 1, "units": 2, "revenue": 3000.0},
    )
    rejected_df = FakeWritableDataFrame(
        1,
        written_paths,
        columns=spark_pipeline.REJECTED_COLUMNS,
        aggregate_row={"orders": 1, "units": 1, "revenue": 800.0},
    )
    spark_outputs = {
        "gold_revenue_metrics": {
            "dataframe": FakeWritableDataFrame(
                1,
                written_paths,
                columns=spark_pipeline.GOLD_REVENUE_FIELDS,
                aggregate_row={"orders": 1, "units": 2, "revenue": 2999.0},
            ),
        },
        "gold_customer_metrics": {
            "dataframe": FakeWritableDataFrame(
                1,
                written_paths,
                columns=spark_pipeline.GOLD_CUSTOMER_FIELDS,
                aggregate_row={"orders": 1, "units": 2, "revenue": 3000.0},
            ),
        },
        "gold_category_metrics": {
            "dataframe": FakeWritableDataFrame(
                1,
                written_paths,
                columns=spark_pipeline.GOLD_CATEGORY_FIELDS,
                aggregate_row={"orders": 1, "units": 2, "revenue": 3000.0},
            ),
        },
        "gold_rejection_metrics": {
            "dataframe": FakeWritableDataFrame(
                1,
                written_paths,
                columns=spark_pipeline.GOLD_REJECTION_FIELDS,
                aggregate_row={"orders": 1, "units": 1, "revenue": 800.0},
            ),
        },
    }

    reconciliation = spark_pipeline.build_spark_metric_reconciliation(
        silver_df,
        rejected_df,
        spark_outputs,
    )

    assert reconciliation["success"] is False
    assert reconciliation["failed_checks"] == [
        "gold_revenue_amount_match_silver"
    ]
    failed_check = {
        check["name"]: check for check in reconciliation["checks"]
    }["gold_revenue_amount_match_silver"]
    assert failed_check == {
        "name": "gold_revenue_amount_match_silver",
        "success": False,
        "expected": 3000.0,
        "actual": 2999.0,
        "difference": 1.0,
    }
    with pytest.raises(
        ValueError,
        match="Metric reconciliation failed: gold_revenue_amount_match_silver",
    ):
        spark_pipeline.raise_for_failed_metric_reconciliation(reconciliation)


def test_spark_run_comparison_reports_output_and_config_deltas():
    previous_manifest = {
        "run": {
            "completed_at_utc": "2026-08-01T00:00:00Z",
            "config_sha256": "old-config",
        },
        "source": {"sha256": "old-source"},
        "config": {
            "included_statuses": ["cancelled", "delivered"],
            "order_date_window": {"start": "2026-07-01", "end": "2026-07-31"},
            "warning_thresholds": {
                "max_rejection_rate": 0.25,
                "min_silver_rows": 10,
            },
        },
        "outputs": {
            "silver_orders": {"rows": 12},
            "rejected_orders": {"rows": 3},
        },
        "output_inventory": {
            "silver_orders": {"sha256": "old-silver"},
            "rejected_orders": {"sha256": "same-rejected"},
        },
        "health": {"status": "passed", "warning_count": 0},
    }
    current_manifest = {
        "run": {
            "completed_at_utc": "2026-08-02T00:00:00Z",
            "config_sha256": "new-config",
        },
        "source": {"sha256": "new-source"},
        "config": {
            "included_statuses": ["delivered", "returned"],
            "order_date_window": {"start": "2026-07-01", "end": "2026-08-01"},
            "warning_thresholds": {
                "max_rejection_rate": 0.2,
                "max_source_lag_days": 7,
            },
        },
        "outputs": {
            "silver_orders": {"rows": 10},
            "rejected_orders": {"rows": 5},
        },
        "output_inventory": {
            "silver_orders": {"sha256": "new-silver"},
            "rejected_orders": {"sha256": "same-rejected"},
        },
        "health": {"status": "warning", "warning_count": 2},
    }

    comparison = spark_pipeline.build_spark_run_comparison(
        current_manifest,
        previous_manifest,
    )

    assert comparison == {
        "version": 1,
        "previous_manifest_available": True,
        "previous_completed_at_utc": "2026-08-01T00:00:00Z",
        "current_completed_at_utc": "2026-08-02T00:00:00Z",
        "source_sha256_changed": True,
        "config_sha256_changed": True,
        "config_scope_changes": {
            "included_statuses": {
                "previous": ["cancelled", "delivered"],
                "current": ["delivered", "returned"],
                "added": ["returned"],
                "removed": ["cancelled"],
                "changed": True,
            },
            "order_date_window": {
                "previous": {"start": "2026-07-01", "end": "2026-07-31"},
                "current": {"start": "2026-07-01", "end": "2026-08-01"},
                "changed": True,
            },
            "warning_thresholds": {
                "max_rejection_rate": {
                    "previous": 0.25,
                    "current": 0.2,
                    "changed": True,
                },
                "max_source_lag_days": {
                    "previous": None,
                    "current": 7,
                    "changed": True,
                },
                "min_silver_rows": {
                    "previous": 10,
                    "current": None,
                    "changed": True,
                },
            },
        },
        "health_status_changed": True,
        "warning_count_delta": 2,
        "output_row_deltas": {
            "rejected_orders": {"previous": 3, "current": 5, "delta": 2},
            "silver_orders": {"previous": 12, "current": 10, "delta": -2},
        },
        "output_checksum_changes": {
            "rejected_orders": {
                "previous_sha256": "same-rejected",
                "current_sha256": "same-rejected",
                "sha256_changed": False,
            },
            "silver_orders": {
                "previous_sha256": "old-silver",
                "current_sha256": "new-silver",
                "sha256_changed": True,
            },
        },
    }


def test_spark_run_comparison_tolerates_missing_previous_manifest():
    comparison = spark_pipeline.build_spark_run_comparison(
        {},
        unavailable_reason="invalid_json",
    )

    assert comparison == {
        "version": 1,
        "previous_manifest_available": False,
        "unavailable_reason": "invalid_json",
    }


def test_spark_run_comparison_tolerates_malformed_sections():
    comparison = spark_pipeline.build_spark_run_comparison(
        {
            "run": {"config_sha256": "current-config"},
            "source": {"sha256": "current-source"},
            "config": {"included_statuses": ["delivered"]},
            "outputs": {"silver_orders": {"rows": 2}},
            "output_inventory": {"silver_orders": {"sha256": "current-silver"}},
        },
        {
            "run": {"config_sha256": "previous-config"},
            "source": {"sha256": "previous-source"},
            "config": ["not", "an", "object"],
            "outputs": ["not", "an", "object"],
            "output_inventory": ["not", "an", "object"],
        },
    )

    assert comparison["previous_manifest_available"] is True
    assert comparison["config_scope_changes"]["included_statuses"] == {
        "previous": [],
        "current": ["delivered"],
        "added": ["delivered"],
        "removed": [],
        "changed": True,
    }
    assert comparison["config_scope_changes"]["order_date_window"] == {
        "previous": {"start": None, "end": None},
        "current": {"start": None, "end": None},
        "changed": False,
    }
    assert comparison["output_row_deltas"] == {
        "silver_orders": {"previous": None, "current": 2, "delta": None},
    }
    assert comparison["output_checksum_changes"] == {
        "silver_orders": {
            "previous_sha256": None,
            "current_sha256": "current-silver",
            "sha256_changed": True,
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


def test_spark_pipeline_fails_before_writing_when_gold_metrics_drift(
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
        columns=spark_pipeline.SILVER_COLUMNS,
        aggregate_row={"orders": 2, "units": 4, "revenue": 8000.0},
    )
    rejected_df = FakeWritableDataFrame(
        1,
        written_paths,
        columns=spark_pipeline.REJECTED_COLUMNS,
        aggregate_row={"orders": 1, "units": 1, "revenue": 800.0},
    )
    FakeSparkSession.reader = FakeSparkReader(raw_df)
    FakeSparkSession.stop_count = 0

    def fake_build_spark_gold_dataframes(*args):
        return {
            "gold_revenue_metrics": {
                "dataframe": FakeWritableDataFrame(
                    1,
                    written_paths,
                    columns=spark_pipeline.GOLD_REVENUE_FIELDS,
                    aggregate_row={"orders": 2, "units": 4, "revenue": 7999.0},
                ),
                "expected_columns": spark_pipeline.GOLD_REVENUE_FIELDS,
                "path_name": "spark_gold_revenue_metrics",
            },
            "gold_customer_metrics": {
                "dataframe": FakeWritableDataFrame(
                    1,
                    written_paths,
                    columns=spark_pipeline.GOLD_CUSTOMER_FIELDS,
                    aggregate_row={"orders": 2, "units": 4, "revenue": 8000.0},
                ),
                "expected_columns": spark_pipeline.GOLD_CUSTOMER_FIELDS,
                "path_name": "spark_gold_customer_metrics",
            },
            "gold_category_metrics": {
                "dataframe": FakeWritableDataFrame(
                    1,
                    written_paths,
                    columns=spark_pipeline.GOLD_CATEGORY_FIELDS,
                    aggregate_row={"orders": 2, "units": 4, "revenue": 8000.0},
                ),
                "expected_columns": spark_pipeline.GOLD_CATEGORY_FIELDS,
                "path_name": "spark_gold_category_metrics",
            },
            "gold_rejection_metrics": {
                "dataframe": FakeWritableDataFrame(
                    1,
                    written_paths,
                    columns=spark_pipeline.GOLD_REJECTION_FIELDS,
                    aggregate_row={"orders": 1, "units": 1, "revenue": 800.0},
                ),
                "expected_columns": spark_pipeline.GOLD_REJECTION_FIELDS,
                "path_name": "spark_gold_rejection_metrics",
            },
        }

    monkeypatch.setattr(spark_pipeline, "_require_pyspark", lambda: FakeSparkSession)
    monkeypatch.setattr(
        spark_pipeline,
        "build_silver_and_rejected_dataframes",
        lambda *args, **kwargs: (silver_df, rejected_df),
    )
    monkeypatch.setattr(
        spark_pipeline,
        "build_spark_gold_dataframes",
        fake_build_spark_gold_dataframes,
    )

    with pytest.raises(
        ValueError,
        match="Metric reconciliation failed: gold_revenue_amount_match_silver",
    ):
        spark_pipeline.run_spark_silver_pipeline(config_path)

    assert written_paths == []
    assert FakeSparkSession.stop_count == 1
    assert not (processed_dir / "spark_pipeline_manifest.json").exists()


def test_spark_pipeline_preserves_existing_outputs_when_staged_write_fails(
    tmp_path,
    monkeypatch,
):
    config_path = tmp_path / "pipeline.json"
    raw_path = tmp_path / "orders.csv"
    processed_dir = tmp_path / "processed"
    existing_silver_file = (
        processed_dir / "spark_silver_orders" / "part-00000.parquet"
    )
    existing_rejected_file = (
        processed_dir / "spark_rejected_orders" / "part-00000.parquet"
    )
    raw_path.write_text(
        "order_id,customer_id,order_date,category,product,quantity,unit_price,status\n",
        encoding="utf-8",
    )
    existing_silver_file.parent.mkdir(parents=True)
    existing_silver_file.write_text("previous silver\n", encoding="utf-8")
    existing_rejected_file.parent.mkdir(parents=True)
    existing_rejected_file.write_text("previous rejected\n", encoding="utf-8")
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
        aggregate_row={"orders": 2, "units": 4, "revenue": 8000.0},
    )
    rejected_df = FakeWritableDataFrame(
        1,
        written_paths,
        columns=spark_pipeline.REJECTED_COLUMNS,
        aggregate_row={"orders": 1, "units": 1, "revenue": 800.0},
    )
    rejected_df.write = FailingDataFrameWriter(written_paths)
    FakeSparkSession.reader = FakeSparkReader(raw_df)
    FakeSparkSession.stop_count = 0

    monkeypatch.setattr(spark_pipeline, "_require_pyspark", lambda: FakeSparkSession)
    monkeypatch.setattr(
        spark_pipeline,
        "build_silver_and_rejected_dataframes",
        lambda *args, **kwargs: (silver_df, rejected_df),
    )

    with pytest.raises(OSError, match="simulated spark write failure"):
        spark_pipeline.run_spark_silver_pipeline(config_path)

    assert existing_silver_file.read_text(encoding="utf-8") == "previous silver\n"
    assert existing_rejected_file.read_text(encoding="utf-8") == "previous rejected\n"
    assert list(processed_dir.glob(".spark_*_orders.staged.*")) == []
    assert not (processed_dir / "spark_pipeline_manifest.json").exists()
    assert FakeSparkSession.stop_count == 1


def test_spark_pipeline_reports_missing_optional_dependency(monkeypatch):
    monkeypatch.setitem(sys.modules, "pyspark", None)
    monkeypatch.setitem(sys.modules, "pyspark.sql", None)

    with pytest.raises(RuntimeError, match="PySpark is required"):
        spark_pipeline._require_pyspark()

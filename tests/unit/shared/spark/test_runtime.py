def test_runtime_factory_shares_one_dependency_graph() -> None:
    from flashflood_data.shared.spark.runtime import build_spark_dependencies

    spark = object()
    store = object()
    meta = object()
    dependencies = build_spark_dependencies(
        spark=spark,
        object_store=store,
        meta=meta,
        catalog_name="flood_lakehouse",
    )

    assert dependencies.snapshots.catalog_name == "flood_lakehouse"
    assert dependencies.staging.store is store
    assert dependencies.audit.meta is meta
    assert dependencies.publisher("silver.dim_basin").spark is spark

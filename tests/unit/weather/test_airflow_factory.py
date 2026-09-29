from flashflood_data.orchestration.weather.planner import verify_weather_outcomes


class LazyOutcomes:
    def __iter__(self):
        yield {"asset_id": "standard-1", "status": "available"}
        yield {"asset_id": "now-1", "status": "available"}


def test_verified_outcomes_materializes_lazy_airflow_mapping_result() -> None:
    plan = {
        "missing": [
            {"asset_id": "standard-1"},
            {"asset_id": "now-1"},
        ]
    }

    result = verify_weather_outcomes(plan, LazyOutcomes())

    assert type(result) is list
    assert result == [
        {"asset_id": "standard-1", "status": "available"},
        {"asset_id": "now-1", "status": "available"},
    ]


def test_verified_outcomes_flattens_mapped_batch_results() -> None:
    plan = {
        "missing": [
            {"asset_id": "standard-1"},
            {"asset_id": "standard-2"},
            {"asset_id": "standard-3"},
        ]
    }

    result = verify_weather_outcomes(
        plan,
        [
            [
                {"asset_id": "standard-1", "status": "available"},
                {"asset_id": "standard-2", "status": "available"},
            ],
            [{"asset_id": "standard-3", "status": "available"}],
        ],
    )

    assert [item["asset_id"] for item in result] == [
        "standard-1",
        "standard-2",
        "standard-3",
    ]

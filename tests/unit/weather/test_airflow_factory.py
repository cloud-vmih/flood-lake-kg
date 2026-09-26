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

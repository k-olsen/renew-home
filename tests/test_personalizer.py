from datetime import datetime

import pytest
from personalizer import Personalizer, Telemetry

DUMMY_TELEM_ROW = Telemetry(
    device_id="dev1",
    local_interval_start_time=datetime(2025, 10, 6, 12, 0),
    cooling_target_temperature_celsius=22.0,
    indoor_temperature_celsius=23.0,
    outdoor_temperature_celsius=30.0,
    duration_user_home_seconds=900,
    duration_cooling_seconds=300,
    schedule_offset_celsius=0.5,
)

DUMMY_DIAL_ROWS = []


def test_rows_to_df():
    personalizer = Personalizer()
    df = personalizer._rows_to_df([DUMMY_TELEM_ROW])
    assert not df.empty
    assert df.iloc[0]["device_id"] == "dev1"


def test_get_offset_events():
    personalizer = Personalizer()
    rows = [
        DUMMY_TELEM_ROW,
        Telemetry(
            device_id="dev1",
            local_interval_start_time=datetime(2025, 10, 6, 12, 15),
            cooling_target_temperature_celsius=22.0,
            indoor_temperature_celsius=23.0,
            outdoor_temperature_celsius=30.0,
            duration_user_home_seconds=900,
            duration_cooling_seconds=300,
            schedule_offset_celsius=0.5,
        ),
        Telemetry(
            device_id="dev1",
            local_interval_start_time=datetime(2025, 10, 6, 13, 0),
            cooling_target_temperature_celsius=22.0,
            indoor_temperature_celsius=23.0,
            outdoor_temperature_celsius=30.0,
            duration_user_home_seconds=900,
            duration_cooling_seconds=300,
            schedule_offset_celsius=0.0,
        ),
    ]
    df = personalizer._get_offset_events(personalizer._rows_to_df(rows))
    assert len(df) == 1
    assert df.iloc[0]["offset_start"] == datetime(2025, 10, 6, 12, 0)
    assert df.iloc[0]["offset_end"] == datetime(2025, 10, 6, 12, 30)


def test_compute_metrics_no_overrides():
    personalizer = Personalizer()
    metrics = personalizer.compute_metrics(
        telem_rows=[DUMMY_TELEM_ROW], dial_turn_rows=DUMMY_DIAL_ROWS
    )
    assert metrics["n_offset_events"] == 1
    assert metrics["n_overrides"] == 0
    assert metrics["override_rate"] == 0.0


def test_calculate_preference():
    personalizer = Personalizer()
    pref = personalizer.calculate_preference(
        telem_rows=[DUMMY_TELEM_ROW], dial_turn_rows=DUMMY_DIAL_ROWS
    )
    assert pref["label"] in ["L", "M", "H"]
    assert pref["offset_celsius"] in [0.5, 0.7, 0.8]
    assert "metrics" in pref


@pytest.mark.parametrize(
    "metrics,expected_label",
    [
        ({"override_rate": 0.6, "n_offset_events": 10}, "L"),
        ({"override_rate": 0.3, "n_offset_events": 10}, "M"),
        ({"override_rate": 0.1, "n_offset_events": 10}, "H"),
    ],
)
def test_score_tolerance_from_metrics(metrics, expected_label):
    personalizer = Personalizer()
    label = personalizer.score_tolerance_from_metrics(metrics)
    assert label == expected_label


# TODO add more tests for edge cases and different scenarios, including:
# both telem and dial data empty
# zero active offsets, small number of offsets
# overrides outside of offset window
# compute metrics on data that contains gaps
# handling empty inputs

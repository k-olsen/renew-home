from dataclasses import dataclass
from datetime import datetime


@dataclass
class Telemetry:
    device_id: str
    local_interval_start_time: datetime
    cooling_target_temperature_celsius: float
    indoor_temperature_celsius: float
    outdoor_temperature_celsius: float
    duration_user_home_seconds: float
    duration_cooling_seconds: float
    schedule_offset_celsius: (
        float  # if non-zero this tells us an offset is occurring during this time
    )


@dataclass
class DialTurns:
    device_id: str
    local_dial_turn_time: datetime
    schedule_offset_celsius: float  # 0 if dial turn not during schedule offset
    initial_cooling_target_temperature_celsius: float
    final_cooling_target_temperature_celsius: float


# New class to store preference
@dataclass
class DevicePreference:
    device_id: str
    tolerance_label: str
    offset_celsius: float
    confidence: float
    last_updated: datetime

from datetime import datetime
from typing import Dict, List, Optional

import ray
from data_models import DevicePreference, DialTurns, Telemetry
from personalizer import OFFSET_LOW, Personalizer


class DevicePreferenceStore:
    """
    Store precomputed per-device preferences.
    In production this could be a database table or Redis cache.
    """

    def __init__(self):
        self.store: Dict[str, DevicePreference] = {}

    def update(self, device_id: str, preference: dict):
        self.store[device_id] = DevicePreference(
            device_id=device_id,
            tolerance_label=preference["label"],
            offset_celsius=preference["offset_celsius"],
            confidence=preference["confidence"],
            last_updated=datetime.now(),
        )

    def get(self, device_id: str) -> DevicePreference:
        return self.store.get(device_id)

    def batch_get(self, device_ids: List[str]) -> List[DevicePreference]:
        return [self.store.get(d) for d in device_ids]


@ray.remote
def compute_pref(device_id, personalizer, telem_rows, dial_rows, as_of):
    return device_id, personalizer.get_preference(telem_rows, dial_rows, as_of)


def precompute_preferences(
    personalizer: Personalizer,
    telem_data: Dict[str, List[Telemetry]],
    dial_data: Dict[str, List[DialTurns]],
    as_of: Optional[datetime] = None,
) -> DevicePreferenceStore:
    """
    Run at regular intervals (e.g., daily or hourly) to precompute and store preferences
    for all devices. Preferences are computed at the device level.

    telem_data: {device_id: List[Telemetry]}
    dial_data: {device_id: List[DialTurns]}
    """
    ray.init()
    futures = [
        compute_pref.remote(
            device_id,
            personalizer,
            telem_data[device_id],
            dial_data.get(device_id, []),
            as_of,
        )
        for device_id in telem_data
    ]
    results = ray.get(futures)
    store = DevicePreferenceStore()
    for device_id, pref in results:
        store.update(device_id, pref)
    return store


def retrieve_preferences(
    store: DevicePreferenceStore, device_ids: List[str]
) -> List[dict]:
    """
    Retrieve stored preference values for each device.

    NOTE: if no preference is found for a device we default to low tolerance
    to be conservative.
    """
    prefs = []
    for device_id in device_ids:
        pref = store.get(device_id)
        if pref is None:
            # fallback to low tolerance
            prefs.append(
                {
                    "device_id": device_id,
                    "offset_celsius": OFFSET_LOW,
                    "tolerance_label": "L",
                    "confidence": 0.0,
                    "last_updated": None,
                }
            )
        else:
            prefs.append(
                {
                    "device_id": device_id,
                    "offset_celsius": pref.offset_celsius,
                    "tolerance_label": pref.tolerance_label,
                    "confidence": pref.confidence,
                    "last_updated": pref.last_updated,
                }
            )
    return prefs

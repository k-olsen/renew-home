from datetime import datetime

from store import DevicePreference, DevicePreferenceStore, retrieve_preferences


def test_retrieve_preferences():
    store = DevicePreferenceStore()
    now = datetime.now()
    store.store["dev1"] = DevicePreference(
        device_id="dev1",
        tolerance_label="L",
        offset_celsius=0.5,
        confidence=0.9,
        last_updated=now,
    )
    store.store["dev2"] = DevicePreference(
        device_id="dev2",
        tolerance_label="H",
        offset_celsius=0.8,
        confidence=1.0,
        last_updated=now,
    )
    device_ids = ["dev1", "dev2"]
    prefs = retrieve_preferences(store, device_ids)

    assert len(prefs) == 2
    assert prefs[0]["device_id"] == "dev1"
    assert prefs[0]["offset_celsius"] == 0.5
    assert prefs[0]["tolerance_label"] == "L"
    assert prefs[0]["last_updated"] >= now
    assert prefs[1]["device_id"] == "dev2"
    assert prefs[1]["offset_celsius"] == 0.8
    assert prefs[1]["tolerance_label"] == "H"
    assert prefs[1]["last_updated"] >= now


# TODO add test cases for:
# retrieving preferences for a device without any stored preference
# retrieving preferences for a device with a stale preference (old last_updated)
# overwrite a preference and ensure it's updated correctly
# list of device_ids where some missing are in store
# handling empty inputs

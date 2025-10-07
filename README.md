# Personalizer algorithm

This repo contains code for computing and storing device-level temperature offset preferences for smart thermostats, based on recent telemetry and user override behavior. 

## Features
- Computes user tolerance and offset preferences using recent device telemetry and dial turn data
- Stores per-device preferences in an in-memory store (could ultimately be extended to use a database or cache)
- Supports batch precomputation and retrieval of preferences for many devices
- Includes example and test code for core logic

## Key Components
- `personalizer.py`: Core logic for computing user tolerance and offset preferences
- `store.py`: In-memory store for device preferences, batch precompute and retrieval utilities
- `data_models.py`: Data classes for telemetry, dial turns, and device preferences
- `tests/`: Initial tests for most components

## Usage
1. Prepare device telemetry and dial turn data (see `data_models.py` for structure)
2. Use `Personalizer` to compute preferences
3. Store and retrieve preferences using `DevicePreferenceStore`
4. Run tests with `pytest`


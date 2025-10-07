from dataclasses import asdict
from datetime import datetime, timedelta
from typing import List, Optional

import numpy as np
import pandas as pd
from data_models import DialTurns, Telemetry

OFFSET_LOW = 0.5
OFFSET_MED = 0.7
OFFSET_HIGH = 0.8

# fraction of events overridden
LOW_TOLERANCE_RATE = 0.50
MED_TOLERANCE_RATE = 0.25

LOOKBACK_DAYS = 14  # Only query data from last N days
HALF_LIFE_DAYS = 7  # for EWMA calc

INTERVAL_MIN = 15

# TODO: add logging


class Personalizer:
    def __init__(
        self,
        lookback_days: int = LOOKBACK_DAYS,
        ewma_half_life_days: int = HALF_LIFE_DAYS,
        override_magnitude_thresh: float = 0.25,
    ):
        self.lookback_days = lookback_days
        self.ewma_half_life_days = ewma_half_life_days
        self.override_magnitude_thresh = override_magnitude_thresh

        # map labels to offsets in degrees C
        self.offset_map = {"L": OFFSET_LOW, "M": OFFSET_MED, "H": OFFSET_HIGH}

    def _rows_to_df(self, rows: list) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([asdict(r) for r in rows])

    def _get_offset_events(self, telem_df: pd.DataFrame) -> pd.DataFrame:
        """
        Create dataframe containing "offset events" from telemetry data. Offset events
        are defined as continuous time periods when a scheduled offset was active.
        """
        cols = ["device_id", "offset_start", "offset_end", "schedule_offset_celsius"]
        if telem_df.empty:
            return pd.DataFrame(columns=cols)

        # filter to only intervals with active offset
        df = telem_df.sort_values("local_interval_start_time").copy()
        df = df.loc[df["schedule_offset_celsius"].fillna(0) != 0]
        if df.empty:
            return pd.DataFrame(columns=cols)

        # find new events when the gap between intervals > INTERVAL_MIN
        df["prev_end"] = df["local_interval_start_time"].shift() + pd.Timedelta(
            seconds=INTERVAL_MIN * 60
        )
        df["new_event"] = (df["local_interval_start_time"] > df["prev_end"]).astype(int)
        df["event_id"] = df["new_event"].cumsum()

        # get offset events from intervals
        events = (
            df.groupby(["device_id", "event_id"])
            .agg(
                offset_start=("local_interval_start_time", "min"),
                offset_end=(
                    "local_interval_start_time",
                    lambda s: s.max() + pd.Timedelta(minutes=INTERVAL_MIN),
                ),
            )
            .reset_index(drop=True)
        )
        return events

    def compute_metrics(
        self,
        telem_rows: List[Telemetry],
        dial_turn_rows: List[DialTurns],
        as_of: Optional[datetime] = None,
    ) -> dict:
        """
        Compute metrics over the specified lookback window. Override rate is computed as
        a time-decay EWMA over offset events, where an event is counted as overridden if a dial turn
        occurs during the offset event.
        """
        # intervals are recorded in device local time, so use that here
        device_local_tz = (
            telem_rows[0].local_interval_start_time.tzinfo if telem_rows else None
        )
        if as_of is None:
            if device_local_tz is not None:
                as_of = datetime.now(device_local_tz)
            else:
                as_of = datetime.now()

        telem_df = self._rows_to_df(telem_rows)
        dial_df = self._rows_to_df(dial_turn_rows)

        # NOTE: upstream query should have only retrieved data within lookback window
        # filtering here just to be safe
        lookback_cutoff = as_of - timedelta(days=self.lookback_days)
        if not telem_df.empty:
            telem_df = telem_df.loc[
                telem_df["local_interval_start_time"] >= lookback_cutoff
            ]
        if not dial_df.empty:
            dial_df = dial_df.loc[dial_df["local_dial_turn_time"] >= lookback_cutoff]

        # get offset events
        offset_events = self._get_offset_events(telem_df)
        n_offset_events = len(offset_events)
        if offset_events.empty or dial_df.empty:
            return {
                "n_offset_events": n_offset_events,
                "override_rate": 0.0,
                "mean_override_magnitude": 0.0,
                "n_overrides": 0,
            }

        # merge dial turns with offset events to see which overrides happened during offsets
        if not dial_df.empty:
            dial_df = dial_df.rename(columns={"local_dial_turn_time": "dial_time"})
            events_w_overrides = pd.merge_asof(
                dial_df.sort_values("dial_time"),
                offset_events.sort_values("offset_start"),
                left_on="dial_time",
                right_on="offset_start",
                direction="backward",
                tolerance=pd.Timedelta(
                    minutes=INTERVAL_MIN
                ),  # only consider dial turns within the offset event
            )
            # count as override if events_w_overrides event exists
            events_w_overrides["is_override"] = events_w_overrides[
                "offset_start"
            ].notnull()
            n_overrides = events_w_overrides["is_override"].sum()
        else:
            n_overrides = 0

        # calc override rate using EWMA
        if n_overrides > 0:
            offset_events["age_in_days"] = (
                as_of - offset_events["offset_start"]
            ).dt.total_seconds() / 86400

            # decay equation
            offset_events["weight"] = np.exp(
                -np.log(2) * offset_events["age_in_days"] / self.ewma_half_life_days
            )

            # did user override this event?
            offset_events["was_overridden"] = offset_events["offset_start"].isin(
                events_w_overrides.loc[
                    events_w_overrides["is_override"], "offset_start"
                ]
            )

            # calc override rate
            ewma_override_rate = (
                offset_events["was_overridden"] * offset_events["weight"]
            ).sum() / offset_events["weight"].sum()
        else:
            ewma_override_rate = 0.0

        return {
            "n_offset_events": n_offset_events,
            "override_rate": ewma_override_rate,
            "n_overrides": n_overrides,
        }

    def score_tolerance_from_metrics(self, metrics) -> str:
        """
        Map override_rate to label
        """
        r = metrics["override_rate"]

        # thresholds
        if r >= LOW_TOLERANCE_RATE:
            label = "L"
        elif r >= MED_TOLERANCE_RATE:
            label = "M"
        else:
            label = "H"

        return label

    def calculate_preference(
        self,
        telem_rows: List[Telemetry],
        dial_turn_rows: List[DialTurns],
        as_of: Optional[datetime] = None,
    ) -> dict:
        """
        Get user tolerance preference based on historical telemetry and dial turn data.
        """
        metrics = self.compute_metrics(telem_rows, dial_turn_rows, as_of=as_of)
        label = self.score_tolerance_from_metrics(metrics)
        offset = self.offset_map[label]
        return {
            "label": label,
            "offset_celsius": offset,
            "metrics": metrics,
        }

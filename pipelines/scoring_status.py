from __future__ import annotations

"""Utility module that maintains the scoring_status ledger used to make the pipeline
idempotent.

The ledger is stored as a Parquet file under ``data_lake/metadata`` so it can be
queried easily by both pandas / Spark / dbt while retaining schema information.

Uniqueness is enforced on the pair *(email, request_id)* coming from the Flinks
payload.  Each row describes the current lifecycle state for a credit-card
application as it moves through the pipeline:

PENDING  -> collected in Silver, ready to be exported to Taktile
SENT     -> payload sent to Taktile, waiting for decision
SCORED   -> decision received and stored in the ledger
ERROR    -> terminal failure – manual intervention needed

The class exposes idempotent helpers so that running the same code twice never
creates duplicates.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

_LEDGER_COLUMNS = [
    "email",
    "request_id",
    "status",
    "payload_hash",
    "sent_timestamp",
    "scored_timestamp",
    "score",
    "limit",
]


class ScoringStatusLedger:
    """Light-weight wrapper around a Parquet-backed Pandas DataFrame."""

    def __init__(self, path: str | Path = "data_lake/metadata/scoring_status.parquet") -> None:
        self.path = Path(path)
        # Ensure parent directory exists so that the first run succeeds.
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._df: pd.DataFrame = self._load()

    # ---------------------------------------------------------------------
    # Public helpers
    # ---------------------------------------------------------------------
    def record_pending(self, email: str, request_id: str, payload_hash: str | None = None) -> None:
        """Insert *(email, request_id)* as **PENDING** if not present already."""
        if self._exists(email, request_id):
            return  # Idempotent – already in the ledger.

        row = {
            "email": email,
            "request_id": request_id,
            "status": "PENDING",
            "payload_hash": payload_hash,
            "sent_timestamp": pd.NaT,
            "scored_timestamp": pd.NaT,
            "score": None,
            "limit": None,
        }
        self._df = pd.concat([self._df, pd.DataFrame([row])], ignore_index=True)
        self._save()

    def mark_sent(self, email: str, request_id: str) -> None:
        """Transition row to **SENT** and stamp current UTC time."""
        idx = self._locate(email, request_id, create_if_missing=True)
        self._df.loc[idx, ["status", "sent_timestamp"]] = [
            "SENT",
            _utc_now(),
        ]
        self._save()

    def mark_scored(
        self,
        email: str,
        request_id: str,
        score: float,
        limit: float,
    ) -> None:
        """Transition row to **SCORED** and capture model outputs."""
        idx = self._locate(email, request_id, create_if_missing=True)
        self._df.loc[idx, [
            "status",
            "scored_timestamp",
            "score",
            "limit",
        ]] = [
            "SCORED",
            _utc_now(),
            score,
            limit,
        ]
        self._save()

    def get_pending(self) -> pd.DataFrame:
        """Return view of applications still waiting to be sent to Taktile."""
        return self._df[self._df["status"] == "PENDING"].copy()

    # ------------------------------------------------------------------
    # Internal utilities
    # ------------------------------------------------------------------
    def _load(self) -> pd.DataFrame:
        if self.path.exists():
            return pd.read_parquet(self.path)
        # Initialise the empty ledger on first run.
        return pd.DataFrame(columns=_LEDGER_COLUMNS)

    def _save(self) -> None:
        # Parquet preserves dtypes better than CSV and is splittable for Spark.
        self._df.to_parquet(self.path, index=False)

    def _exists(self, email: str, request_id: str) -> bool:
        return (
            (self._df["email"] == email) & (self._df["request_id"] == request_id)
        ).any()

    def _locate(self, email: str, request_id: str, *, create_if_missing: bool = False) -> pd.Index:
        mask = (self._df["email"] == email) & (self._df["request_id"] == request_id)
        if not mask.any():
            if not create_if_missing:
                raise KeyError(f"({email}, {request_id}) not present in ledger")
            # Auto-create PENDING row so state machine still works.
            self.record_pending(email, request_id)
            mask = (self._df["email"] == email) & (self._df["request_id"] == request_id)
        return self._df[mask].index


def _utc_now() -> datetime:
    """Return timezone-aware timestamp in UTC suitable for Parquet."""
    return datetime.now(timezone.utc) 

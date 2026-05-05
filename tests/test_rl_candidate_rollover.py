from __future__ import annotations

from datetime import datetime
from datetime import timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.orchestration.rl_candidate_rollover import (
    archive_expired_rl_candidates,
)


def _candidate_record(
    instruction_id: str,
    *,
    expire_at: datetime,
) -> InstructionRecord:
    return InstructionRecord(
        instruction_id=instruction_id,
        schema_version="2026-04-25",
        source_system="upstream-agent",
        batch_id="candidate-batch",
        account_key="VIRTUALRL01",
        book_key="rl_shared_long_trial_106_virtual_01",
        is_virtual=True,
        symbol="AXFO",
        exchange="XSTO",
        currency="SEK",
        state="MODEL_ROUTED_PENDING",
        submit_at=datetime(2026, 5, 4, 7, 0, tzinfo=timezone.utc),
        expire_at=expire_at,
        order_type="MODEL_ROUTED",
        side="BUY",
        payload={
            "instruction": {
                "instruction_id": instruction_id,
                "execution": {
                    "mode": "model_routed",
                    "model_id": "long_trial_106_v1",
                },
            }
        },
    )


class RlCandidateRolloverTests(TestCase):
    def test_archives_only_expired_model_routed_source_candidates(self) -> None:
        with TemporaryDirectory() as temp_dir:
            database_url = f"sqlite+pysqlite:///{Path(temp_dir) / 'rollover.db'}"
            engine = build_engine(database_url)
            create_schema(engine)
            session_factory = create_session_factory(engine)
            cutoff = datetime(2026, 5, 4, 16, 0, tzinfo=timezone.utc)
            session = session_factory()
            try:
                session.add(
                    _candidate_record(
                        "expired-candidate",
                        expire_at=datetime(2026, 5, 4, 15, 30, tzinfo=timezone.utc),
                    )
                )
                session.add(
                    _candidate_record(
                        "future-candidate",
                        expire_at=datetime(2026, 5, 5, 15, 30, tzinfo=timezone.utc),
                    )
                )
                session.add(
                    InstructionRecord(
                        instruction_id="generated-position",
                        schema_version="2026-04-10",
                        source_system="rl-runner",
                        batch_id="generated-batch",
                        account_key="VIRTUALRL01",
                        book_key="rl_shared_long_trial_106_virtual_01",
                        is_virtual=True,
                        symbol="AXFO",
                        exchange="XSTO",
                        currency="SEK",
                        state="POSITION_OPEN",
                        submit_at=datetime(2026, 5, 4, 7, 0, tzinfo=timezone.utc),
                        expire_at=datetime(2026, 5, 5, 7, 0, tzinfo=timezone.utc),
                        order_type="LIMIT",
                        side="BUY",
                        payload={
                            "instruction": {
                                "instruction_id": "generated-position",
                                "trace": {
                                    "metadata": {
                                        "rl_source_instruction_id": "expired-candidate"
                                    }
                                },
                            }
                        },
                    )
                )
                session.commit()
            finally:
                session.close()

            result = archive_expired_rl_candidates(
                session_factory,
                cutoff=cutoff,
                requested_by="test",
            )

            self.assertEqual(result.archived_candidate_count, 1)
            self.assertEqual(result.candidate_ids, ("expired-candidate",))

            session = session_factory()
            try:
                rows = {
                    row.instruction_id: row
                    for row in session.query(InstructionRecord).all()
                }
                self.assertIsNotNone(rows["expired-candidate"].archived_at)
                self.assertEqual(rows["expired-candidate"].archived_by, "test")
                self.assertIsNone(rows["future-candidate"].archived_at)
                self.assertIsNone(rows["generated-position"].archived_at)

                events = session.query(InstructionEventRecord).all()
                self.assertEqual(len(events), 1)
                self.assertEqual(events[0].event_type, "rl_candidate_archived")
                self.assertEqual(events[0].source, "rl_candidate_rollover")
            finally:
                session.close()
                engine.dispose()

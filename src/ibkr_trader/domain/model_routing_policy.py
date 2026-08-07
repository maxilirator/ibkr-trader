from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetiredModelRoute:
    model_id: str
    account_key: str
    book_key: str
    reason: str

    def matches(self, *, model_id: str, account_key: str, book_key: str) -> bool:
        return (
            _normalize(model_id) == _normalize(self.model_id)
            and _normalize(account_key) == _normalize(self.account_key)
            and _normalize(book_key) == _normalize(self.book_key)
        )


LONG_TRIAL_106_SEEDPICKER_RETIREMENT_REASON = (
    "long_trial_106_v1 is retired for VIRTUALSEEDRL01/seedpicker_rl_long_01: "
    "the checkpoint requires the full research-compatible multi-name "
    "market-context universe, so it is not fit for the single-name seedpicker "
    "virtual RL path. Submit a retrained single-name seedpicker model, or use a "
    "new deployment that supplies the full panel context."
)


RETIRED_MODEL_ROUTES: tuple[RetiredModelRoute, ...] = (
    RetiredModelRoute(
        model_id="long_trial_106_v1",
        account_key="VIRTUALSEEDRL01",
        book_key="seedpicker_rl_long_01",
        reason=LONG_TRIAL_106_SEEDPICKER_RETIREMENT_REASON,
    ),
)


def retired_model_route_reason(
    *,
    model_id: str,
    account_key: str,
    book_key: str,
) -> str | None:
    for route in RETIRED_MODEL_ROUTES:
        if route.matches(
            model_id=model_id,
            account_key=account_key,
            book_key=book_key,
        ):
            return route.reason
    return None


def _normalize(value: str) -> str:
    return str(value).strip().lower()

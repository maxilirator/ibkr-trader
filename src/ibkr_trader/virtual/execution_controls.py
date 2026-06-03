from __future__ import annotations

from ibkr_trader.virtual.execution_core import *

def cancel_virtual_order(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    order_id: int,
    *,
    timeout: int = 10,
) -> dict[str, Any]:
    del broker_config, timeout
    with session_scope(session_factory) as session:
        broker_order = session.execute(
            select(BrokerOrderRecord).where(
                BrokerOrderRecord.broker_kind == BROKER_KIND_VIRTUAL,
                BrokerOrderRecord.external_order_id == str(order_id),
            )
        ).scalar_one_or_none()
        if broker_order is None:
            return {
                "broker_kind": BROKER_KIND_VIRTUAL,
                "is_virtual": True,
                "broker_order_status": {
                    "orderId": order_id,
                    "status": "NOT_FOUND_AT_BROKER",
                },
                "warning": "Virtual order was already absent at cancel time.",
            }
        if _is_closed_status(broker_order.status):
            status = broker_order.status
        else:
            previous_status = broker_order.status
            status = "Cancelled"
            broker_order.status = status
            broker_order.last_status_at = utc_now()
            session.add(
                BrokerOrderEventRecord(
                    broker_order_id=broker_order.id,
                    event_type="virtual_order_cancelled",
                    event_at=broker_order.last_status_at,
                    status_before=previous_status,
                    status_after=status,
                    payload={"order_id": order_id, "is_virtual": True},
                    note="Virtual order cancelled without contacting IBKR.",
                )
            )
        return {
            "broker_kind": BROKER_KIND_VIRTUAL,
            "is_virtual": True,
            "account": broker_order.account_key,
            "broker_order_status": {
                "orderId": order_id,
                "status": status,
                "filled": "0",
                "remaining": broker_order.total_quantity or "1",
                "avgFillPrice": "0",
                "permId": (
                    int(broker_order.external_perm_id)
                    if broker_order.external_perm_id not in (None, "")
                    else None
                ),
                "parentId": 0,
                "lastFillPrice": "0",
                "clientId": (
                    int(broker_order.external_client_id)
                    if broker_order.external_client_id not in (None, "")
                    else 0
                ),
                "whyHeld": "",
                "mktCapPrice": "0",
            },
        }


def has_real_broker_work(
    session_factory: sessionmaker[Session],
    *,
    instruction_ids: tuple[str, ...] | None = None,
) -> bool:
    with session_scope(session_factory) as session:
        active_instruction = select(InstructionRecord.id).where(
            InstructionRecord.state.in_(
                ("ENTRY_SUBMITTED", "POSITION_OPEN", "EXIT_PENDING")
            ),
            InstructionRecord.is_virtual.is_(False),
        )
        if instruction_ids:
            active_instruction = active_instruction.where(
                InstructionRecord.instruction_id.in_(instruction_ids)
            )
        if session.execute(active_instruction.limit(1)).first() is not None:
            return True

        unsettled_order = select(BrokerOrderRecord.id).where(
            BrokerOrderRecord.is_virtual.is_(False),
            _open_virtual_order_status_clause(),
        )
        return session.execute(unsettled_order.limit(1)).first() is not None


def is_virtual_instruction(instruction: ExecutionInstruction) -> bool:
    return is_virtual_account_key(instruction.account.account_key)

__all__ = [name for name in globals() if not name.startswith("__")]

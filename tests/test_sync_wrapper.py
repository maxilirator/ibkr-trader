from __future__ import annotations

import logging
import time
from types import SimpleNamespace
from unittest import TestCase

from ibkr_trader.ibkr.sync_wrapper import load_sync_wrapper_class


class SyncWrapperTests(TestCase):
    def test_informational_ibkr_messages_do_not_log_as_errors(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)
        logger = logging.getLogger("ibapi.wrapper")
        records: list[logging.LogRecord] = []

        class _ListHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                records.append(record)

        handler = _ListHandler()
        original_level = logger.level
        logger.addHandler(handler)
        logger.setLevel(logging.ERROR)
        try:
            app.error(-1, 0, 2104, "Market data farm connection is OK:eufarm")
            app.error(
                -1,
                0,
                2107,
                "HMDS data farm connection is inactive but should be available upon demand.euhmds",
            )
            app.error(-1, 0, 2119, "Market data farm is connecting:eufarmnj")
            app.error(-1, 0, 2158, "Sec-def data farm connection is OK:secdefeu")
        finally:
            logger.removeHandler(handler)
            logger.setLevel(original_level)

        self.assertEqual(records, [])

    def test_connect_and_start_waits_for_next_valid_id(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)
        connect_calls: list[tuple[str, int, int]] = []

        def fake_connect(host: str, port: int, client_id: int) -> None:
            connect_calls.append((host, port, client_id))

        def fake_is_connected() -> bool:
            return True

        def fake_run() -> None:
            time.sleep(0.05)
            app.next_valid_id_value = 101

        app.connect = fake_connect  # type: ignore[method-assign]
        app.isConnected = fake_is_connected  # type: ignore[method-assign]
        app.run = fake_run  # type: ignore[method-assign]

        connected = app.connect_and_start("127.0.0.1", 4001, 7)

        self.assertTrue(connected)
        self.assertEqual(connect_calls, [("127.0.0.1", 4001, 7)])
        self.assertEqual(app.next_valid_id_value, 101)

        app.disconnect_and_stop()

    def test_connect_and_start_fails_when_next_valid_id_never_arrives(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)
        disconnect_calls = 0

        def fake_connect(host: str, port: int, client_id: int) -> None:
            return None

        def fake_is_connected() -> bool:
            return True

        def fake_run() -> None:
            time.sleep(0.05)

        def fake_disconnect_and_stop() -> None:
            nonlocal disconnect_calls
            disconnect_calls += 1

        app.connect = fake_connect  # type: ignore[method-assign]
        app.isConnected = fake_is_connected  # type: ignore[method-assign]
        app.run = fake_run  # type: ignore[method-assign]
        app.disconnect_and_stop = fake_disconnect_and_stop  # type: ignore[method-assign]

        connected = app.connect_and_start("127.0.0.1", 4001, 7)

        self.assertFalse(connected)
        self.assertEqual(disconnect_calls, 1)

    def test_drain_broker_callback_events_records_live_order_callbacks(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)

        app.openOrder(
            11,
            SimpleNamespace(
                symbol="AAPL",
                localSymbol="AAPL",
                secType="STK",
                exchange="SMART",
                primaryExchange="NASDAQ",
                currency="USD",
            ),
            SimpleNamespace(
                permId=9001,
                clientId=0,
                account="DU1234567",
                orderRef="sync-aapl-1",
                action="BUY",
                totalQuantity="1",
                orderType="LMT",
                lmtPrice="200.00",
                auxPrice="",
                outsideRth=False,
                transmit=True,
            ),
            SimpleNamespace(
                status="PreSubmitted",
                warningText="Held in TWS.",
                rejectReason="",
                completedStatus="",
                completedTime="",
            ),
        )
        app.orderStatus(
            11,
            "Submitted",
            "0",
            "1",
            0.0,
            9001,
            0,
            0.0,
            0,
            "",
            0.0,
        )
        app.error(11, 0, 202, "Rejected by exchange", '{"reason":"test"}')

        events = app.drain_broker_callback_events()

        self.assertEqual(
            [item["event_type"] for item in events],
            ["open_order", "order_status", "order_error"],
        )
        self.assertEqual(events[0]["order"]["order_ref"], "sync-aapl-1")
        self.assertEqual(events[1]["order_status"]["status"], "Submitted")
        self.assertEqual(events[2]["error"]["errorCode"], 202)
        self.assertEqual(app.drain_broker_callback_events(), [])

    def test_suppressed_open_order_callbacks_do_not_hit_journal(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)

        with app._suppress_broker_callback_events("open_order", "order_status"):
            app.openOrder(
                11,
                SimpleNamespace(
                    symbol="AAPL",
                    localSymbol="AAPL",
                    secType="STK",
                    exchange="SMART",
                    primaryExchange="NASDAQ",
                    currency="USD",
                ),
                SimpleNamespace(
                    permId=9001,
                    clientId=0,
                    account="DU1234567",
                    orderRef="sync-aapl-1",
                    action="BUY",
                    totalQuantity="1",
                    orderType="LMT",
                    lmtPrice="200.00",
                    auxPrice="",
                    outsideRth=False,
                    transmit=True,
                ),
                SimpleNamespace(
                    status="PreSubmitted",
                    warningText="Held in TWS.",
                    rejectReason="",
                    completedStatus="",
                    completedTime="",
                ),
            )
            app.orderStatus(
                11,
                "Submitted",
                "0",
                "1",
                0.0,
                9001,
                0,
                0.0,
                0,
                "",
                0.0,
            )

        self.assertEqual(app.drain_broker_callback_events(), [])

    def test_get_account_summary_cancels_subscription_on_error(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)
        cancelled_request_ids: list[int] = []

        app._next_local_request_id = lambda: 41  # type: ignore[method-assign]
        app.reqAccountSummary = lambda req_id, group, tags: None  # type: ignore[method-assign]
        app.cancelAccountSummary = cancelled_request_ids.append  # type: ignore[method-assign]

        def fake_wait_for_response(req_id: int, response_name: str, timeout: int):
            raise TimeoutError("timed out")

        app._wait_for_response = fake_wait_for_response  # type: ignore[method-assign]

        with self.assertRaisesRegex(TimeoutError, "timed out"):
            app.get_account_summary("NetLiquidation", timeout=5)

        self.assertEqual(cancelled_request_ids, [41])

    def test_preview_order_sync_marks_whatif_orders_transmitted(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)
        contract = SimpleNamespace(symbol="HEM", secType="STK")
        order = SimpleNamespace(whatIf=False, transmit=False)
        placed: list[tuple[int, object, object]] = []

        app._request_next_order_id_from_broker = lambda timeout=None: 41  # type: ignore[method-assign]
        app.placeOrder = lambda order_id, contract, order: placed.append(  # type: ignore[method-assign]
            (order_id, contract, order)
        )
        app._wait_for_response = lambda order_id, response_name, timeout: {  # type: ignore[method-assign]
            "orderId": order_id,
            "contract": contract,
            "order": order,
            "orderState": SimpleNamespace(status="PreSubmitted"),
        }

        result = app.preview_order_sync(contract, order, timeout=5)

        self.assertEqual(result["orderId"], 41)
        self.assertEqual(placed, [(41, contract, order)])
        self.assertTrue(order.whatIf)
        self.assertTrue(order.transmit)

    def test_place_order_sync_records_outbound_wire_audit(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)
        contract = SimpleNamespace(
            conId=483984824,
            symbol="HEM",
            localSymbol="HEM",
            tradingClass="HEM",
            secType="STK",
            exchange="SMART",
            primaryExchange="SFB",
            currency="SEK",
            secIdType="ISIN",
            secId="SE0015671995",
        )
        order = SimpleNamespace(
            account="U25245596",
            orderRef="wire-audit-entry-1",
            action="BUY",
            orderType="LMT",
            totalQuantity=1,
            lmtPrice=93.9,
            auxPrice=None,
            tif="DAY",
            outsideRth=False,
            transmit=True,
            whatIf=False,
            ocaGroup=None,
            ocaType=None,
        )

        app._request_next_order_id_from_broker = lambda timeout=None: 41  # type: ignore[method-assign]
        app.serverVersion = lambda: 200  # type: ignore[method-assign]
        app.sendMsg = lambda message: None  # type: ignore[method-assign]
        app._wait_for_response = lambda order_id, response_name, timeout: {  # type: ignore[method-assign]
            "orderId": order_id,
            "status": "Submitted",
        }

        app.place_order_sync(contract, order, timeout=5)

        events = app.broker_wire_audit_events_since(0)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_type"], "outbound_order_request")
        self.assertEqual(events[0]["request"]["api_method"], "placeOrder")
        self.assertEqual(events[0]["request"]["stage"], "live_order_submit")
        self.assertEqual(events[0]["request"]["order_id"], 41)
        self.assertEqual(events[0]["request"]["order"]["order_ref"], "wire-audit-entry-1")
        self.assertEqual(events[0]["request"]["order"]["limit_price"], "93.9")
        self.assertTrue(events[0]["request"]["order"]["transmit"])
        self.assertFalse(events[0]["request"]["order"]["what_if"])
        self.assertEqual(events[0]["request"]["contract"]["sec_id"], "SE0015671995")

    def test_cancel_order_sync_records_outbound_wire_audit(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)
        app.serverVersion = lambda: 200  # type: ignore[method-assign]
        app.sendMsg = lambda message: None  # type: ignore[method-assign]
        app._wait_for_response = lambda order_id, response_name, timeout: {  # type: ignore[method-assign]
            "orderId": order_id,
            "status": "Cancelled",
        }

        app.cancel_order_sync(41, timeout=5)

        events = app.broker_wire_audit_events_since(0)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_type"], "outbound_cancel_request")
        self.assertEqual(events[0]["request"]["api_method"], "cancelOrder")
        self.assertEqual(events[0]["request"]["order_id"], 41)

    def test_send_msg_records_raw_order_protocol_payloads(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)
        sent_messages: list[object] = []
        app.serverVersion = lambda: 200  # type: ignore[method-assign]
        app.conn = SimpleNamespace(sendMsg=sent_messages.append)

        app.sendMsg(3, "41\0HEM\0STK\0SMART\0BUY\0")
        app.sendMsg(99, "ignored\0")
        app.sendMsgProtoBuf(204, b"\x08\x96&")

        events = app.broker_wire_audit_events_since(0)
        self.assertEqual(len(sent_messages), 3)
        self.assertEqual(
            [event["event_type"] for event in events],
            ["outbound_ibapi_message", "outbound_ibapi_message"],
        )
        self.assertEqual(events[0]["request"]["api_method"], "sendMsg")
        self.assertEqual(events[0]["request"]["message_id"], 3)
        self.assertEqual(events[0]["request"]["message_name"], "PLACE_ORDER")
        self.assertEqual(events[0]["request"]["payload"]["encoding"], "ibapi_field_string")
        self.assertEqual(events[0]["request"]["payload"]["fields"][:5], ["41", "HEM", "STK", "SMART", "BUY"])
        self.assertEqual(events[1]["request"]["api_method"], "sendMsgProtoBuf")
        self.assertEqual(events[1]["request"]["message_id"], 204)
        self.assertEqual(events[1]["request"]["message_name"], "CANCEL_ORDER_PROTOBUF")
        self.assertEqual(events[1]["request"]["payload"]["encoding"], "protobuf_bytes_base64")
        self.assertEqual(events[1]["request"]["payload"]["base64"], "CJYm")

    def test_order_id_allocator_advances_after_whatif_order(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)
        app._request_next_order_id_from_broker = lambda timeout=None: 41  # type: ignore[method-assign]

        preview_order_id = app._allocate_order_id()
        live_order_id = app._allocate_order_id()

        self.assertEqual(preview_order_id, 41)
        self.assertEqual(live_order_id, 42)

    def test_open_order_callback_wakes_whatif_order_waiter(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)
        contract = SimpleNamespace(symbol="HEM", secType="STK")
        order = SimpleNamespace(orderId=41, whatIf=True, transmit=True)
        order_state = SimpleNamespace(status="PreSubmitted")

        app.openOrder(41, contract, order, order_state)

        result = app._wait_for_response(41, "open_orders", timeout=0.01)

        self.assertEqual(result["orderId"], 41)
        self.assertIs(result["contract"], contract)
        self.assertIs(result["order"], order)
        self.assertIs(result["orderState"], order_state)

    def test_get_account_updates_unsubscribes_on_error(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)
        update_calls: list[tuple[bool, str]] = []

        def fake_req_account_updates(subscribe: bool, account_code: str) -> None:
            update_calls.append((subscribe, account_code))

        def fake_wait_for_response(req_id: int, response_name: str, timeout: int):
            raise TimeoutError("portfolio timed out")

        app.reqAccountUpdates = fake_req_account_updates  # type: ignore[method-assign]
        app._wait_for_response = fake_wait_for_response  # type: ignore[method-assign]

        with self.assertRaisesRegex(TimeoutError, "portfolio timed out"):
            app.get_account_updates("U25245596", timeout=5)

        self.assertEqual(
            update_calls,
            [(True, "U25245596"), (False, "U25245596")],
        )

    def test_commission_and_fees_report_is_merged_into_executions(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)

        app._next_local_request_id = lambda: 41  # type: ignore[method-assign]
        app.reqExecutions = lambda req_id, exec_filter: None  # type: ignore[method-assign]
        app.execution_commissions["exec-001"] = SimpleNamespace(
            execId="exec-001",
            commissionAndFees="1.25",
            currency="SEK",
            realizedPNL="0.00",
        )

        def fake_wait_for_response(req_id: int, response_name: str, timeout: int):
            return [
                {
                    "contract": SimpleNamespace(symbol="SIVE"),
                    "execution": SimpleNamespace(execId="exec-001"),
                }
            ]

        app._wait_for_response = fake_wait_for_response  # type: ignore[method-assign]

        executions = app.get_executions(timeout=5)

        self.assertEqual(len(executions), 1)
        self.assertEqual(
            executions[0]["commission_and_fees_report"].commissionAndFees,
            "1.25",
        )

    def test_commission_and_fees_report_caches_by_execution_id(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)

        app.commissionAndFeesReport(
            SimpleNamespace(
                execId="exec-002",
                commissionAndFees="0.95",
                currency="USD",
                realizedPNL="4.20",
            )
        )

        self.assertEqual(app.execution_commissions["exec-002"].currency, "USD")
        self.assertEqual(app.execution_commissions["exec-002"].commissionAndFees, "0.95")

    def test_get_executions_cleans_up_request_buffer_after_success(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)

        app._next_local_request_id = lambda: 41  # type: ignore[method-assign]
        app.reqExecutions = lambda req_id, exec_filter: None  # type: ignore[method-assign]

        def fake_wait_for_response(req_id: int, response_name: str, timeout: int):
            app.executions[req_id] = [{"execution": SimpleNamespace(execId="exec-001")}]
            return list(app.executions[req_id])

        app._wait_for_response = fake_wait_for_response  # type: ignore[method-assign]

        executions = app.get_executions(timeout=5)

        self.assertEqual(len(executions), 1)
        self.assertNotIn(41, app.executions)
        self.assertIn(41, app._closed_execution_request_ids_lookup)

    def test_exec_details_ignores_late_callbacks_for_closed_request(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)

        app._mark_execution_request_closed(41)

        app.execDetails(
            41,
            SimpleNamespace(symbol="SIVE"),
            SimpleNamespace(execId="exec-late"),
        )
        app.execDetailsEnd(41)

        self.assertNotIn(41, app.executions)

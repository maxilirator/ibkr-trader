# IBKR Order Wire Audit - 2026-05-24

This note captures the actual IBKR API send boundary for the HEXA B limit-order
probe and the follow-up cancel.

## Send Boundary

The repo calls `placeOrder(...)` here:

- `src/ibkr_trader/ibkr/sync_wrapper.py:907` for live order submit.
- `src/ibkr_trader/ibkr/sync_wrapper.py:935` for WhatIf preflight.

The repo audit hook records the order object, then delegates to IBKR:

```python
def placeOrder(self, orderId: int, contract: Any, order: Any) -> None:
    self._record_known_order_id(orderId)
    self._append_broker_wire_audit_event(
        self._serialize_outbound_order_request(orderId, contract, order)
    )
    super().placeOrder(orderId, contract, order)
```

On the installed IBKR Python API on quant, the actual socket send is:

```python
def placeOrderProtoBuf(self, placeOrderRequestProto):
    serializedString = placeOrderRequestProto.SerializeToString()
    self.sendMsgProtoBuf(OUT.PLACE_ORDER + PROTOBUF_MSG_ID, serializedString)

def sendMsgProtoBuf(self, msgId: int, msg: bytes):
    full_msg = comm.make_msg_proto(msgId, msg)
    self.conn.sendMsg(full_msg)
```

For orders, `OUT.PLACE_ORDER + PROTOBUF_MSG_ID` is message id `203`.
For cancels, `OUT.CANCEL_ORDER + PROTOBUF_MSG_ID` is message id `204`.

## Live Order Object Sent

Artifact on quant:

```text
/tmp/ibkr-wire-audit/submit.json
```

The live submit for order `4886` sent this object into IBKR:

```json
{
  "api_method": "placeOrder",
  "stage": "live_order_submit",
  "order_id": 4886,
  "contract": {
    "con_id": 490414358,
    "symbol": "HEXA.B",
    "local_symbol": "HEXA B",
    "trading_class": "HEXA.B",
    "security_type": "STK",
    "exchange": "SMART",
    "primary_exchange": "SFB",
    "currency": "SEK"
  },
  "order": {
    "account": "U25245596",
    "order_ref": "2026-05-24-U25245596-wire-audit-HEXA-B-limit-01",
    "action": "BUY",
    "order_type": "LMT",
    "total_quantity": "1",
    "limit_price": "80.0",
    "time_in_force": "DAY",
    "outside_rth": false,
    "transmit": true,
    "what_if": false
  }
}
```

Raw protobuf capture was added after this live submit. Therefore the old live
submit has the exact `placeOrder` object above, but not captured raw bytes.

## Captured PlaceOrder Protobuf

Artifact on quant:

```text
/tmp/ibkr-wire-audit/whatif_place_after_raw_audit.json
```

This was a WhatIf-only limit-order send using the same contract/order shape.
It did not create a live order.

Raw message:

```json
{
  "api_method": "sendMsgProtoBuf",
  "message_id": 203,
  "message_name": "PLACE_ORDER_PROTOBUF",
  "payload": {
    "encoding": "protobuf_bytes_base64",
    "byte_length": 238,
    "base64": "CAESNAiWwuzpARIGSEVYQS5CGgNTVEtCBVNNQVJUSgNTRkJSA1NFS1oGSEVYQSBCYgZIRVhBLkIasQEIABgAIAAqA0JVWTIBMTgAQgNMTVRJAAAAAAAAVEBaA0RBWWIJVTI1MjQ1NTk24gE1MjAyNi0wNS0yNC1VMjUyNDU1OTYtd2lyZS1hdWRpdC1IRVhBLUItd2hhdGlmLW9ubHktMDHwAQD4AQDYAgDwAgCIBAGQBAGoBACwBADABP///////////wHhBAAAAAAAAAAAwAUAyQUAAAAAAAAAANkFAAAAAAAAAACQBgDKBgAiAA=="
  }
}
```

Decoded protobuf:

```json
{
  "orderId": 1,
  "contract": {
    "conId": 490414358,
    "symbol": "HEXA.B",
    "secType": "STK",
    "exchange": "SMART",
    "primaryExch": "SFB",
    "currency": "SEK",
    "localSymbol": "HEXA B",
    "tradingClass": "HEXA.B"
  },
  "order": {
    "account": "U25245596",
    "action": "BUY",
    "orderType": "LMT",
    "totalQuantity": "1",
    "lmtPrice": 80.0,
    "tif": "DAY",
    "orderRef": "2026-05-24-U25245596-wire-audit-HEXA-B-whatif-only-01",
    "transmit": true,
    "whatIf": true
  }
}
```

## Captured Cancel Protobuf

Artifact on quant:

```text
/tmp/ibkr-wire-audit/cancel_after_raw_audit_v2.json
```

Raw message:

```json
{
  "api_method": "sendMsgProtoBuf",
  "message_id": 204,
  "message_name": "CANCEL_ORDER_PROTOBUF",
  "payload": {
    "encoding": "protobuf_bytes_base64",
    "byte_length": 5,
    "base64": "CJYmEgA="
  }
}
```

Decoded protobuf:

```json
{
  "orderId": 4886,
  "orderCancel": {}
}
```

The broker still reported order `4886` as `PendingCancel` after the cancel send.

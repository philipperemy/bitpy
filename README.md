## ByBit - Derivatives V3 - Unified Margin API

A tiny library made by traders for traders in python.

### Scope

- Websocket API - Public + Private
   - Tickers
   - Order Books (reconstruction)
   - Order Status / Positions / Executions (private feed)
- REST API
   - Positions
   - Order Books
   - Open Orders
   - Orders
   - Tickers
   - New Order
   - Replace Order
   - Cancel Order
   - Mass Cancel Orders
   - Get Balances
- Unified Interface: REST + Websocket.
   - Order books fetched from the websocket if available otherwise from the REST
   - Tickers fetched from the websocket if available otherwise from the REST

It has been tested on perpetuals (linear), not on options.

### Installation

*Via PyPI*

```
pip install bitpy
```

### Usage

The example covers:
- Authenticate to ByBit Unified Account V3 API.
- Log the available balance / equity.
- Cancel all open orders.
- Send new order BUY 0.01 BTCUSDT @ BEST_BID * 0.95.
- Fetch open orders.
- Replace this order 0.02 @ BEST_BID * 0.97.
- Cancel this order.
- Fetch positions.
- Fetch order info.

```python
symbol = 'BTCUSDT'
bybit = ByBit(credentials=CREDENTIALS_FILE)

logging.info(bybit.get_balances())

bybit.cancel_all_orders()

ticker = bybit.get_tickers(symbol=symbol)
bid = ticker['bidPrice']

# Place Order BUY 0.01 BTCUSDT @ BID*0.95.
order = bybit.place_order(symbol=symbol, side='buy', price=bid * 0.95, size=0.01)

# Check that one order is outstanding.
for opened_order in bybit.get_open_orders(symbol=symbol):
    logging.info(opened_order)

# Replace Order BUY 0.02 BTCUSDT @ BID*0.97.
bybit.modify_order(order_id=order['orderId'], price=bid * 0.97, size=0.02)

# Cancel order.
bybit.cancel_order(order_id=order['orderId'])

time.sleep(1)
# Check no positions are present.
for pos in bybit.get_positions(symbol=symbol):
    logging.info(f'Position: {pos["size"]} {pos["symbol"]}.')

# Inspect our orders.
for order_status in bybit.get_orders(symbol=symbol, order_id=order['orderId']):
    logging.info(order_status)
```

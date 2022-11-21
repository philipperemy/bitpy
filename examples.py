import logging
import os
import sys
import time
from pathlib import Path

from bitpy import ByBit

credentials_dir = Path(os.environ['BYBIT_CREDENTIALS_DIR']).expanduser()
CREDENTIALS_FILE = credentials_dir / 'bybit.json'


# JSON content of bybit.json:
# {
#   "apiKey": ".....",
#   "secret": "...."
# }

# Go to https://www.bybit.com/en-US/
# Put 5K USDT on the Derivatives Account.
# Upgrade to Unified Margin Account.


def main():
    symbol = 'BTCUSDT'
    bybit = ByBit(credentials=CREDENTIALS_FILE)

    logging.info(bybit.get_balances())

    bybit.cancel_all_orders()

    ticker = bybit.get_tickers(symbol=symbol)
    bid = ticker['bidPrice']

    # Place Order BUY 0.01 BTCUSDT @ BID*0.95
    order = bybit.place_order(symbol=symbol, side='buy', price=bid * 0.95, size=0.01)

    for opened_order in bybit.get_open_orders(symbol=symbol):
        logging.info(opened_order)

    # Replace Order BUY 0.02 BTCUSDT @ BID*0.97
    bybit.modify_order(order_id=order['orderId'], price=bid * 0.97, size=0.02)
    # Cancel order.
    bybit.cancel_order(order_id=order['orderId'])

    time.sleep(1)
    for pos in bybit.get_positions(symbol=symbol):
        logging.info(f'Position: {pos["size"]} {pos["symbol"]}.')

    for order_status in bybit.get_orders(symbol=symbol, order_id=order['orderId']):
        logging.info(order_status)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
    main()

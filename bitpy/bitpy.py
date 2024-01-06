import hashlib
import hmac
import json
import logging
import queue
import tempfile
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from enum import Enum
from pathlib import Path
from typing import Optional, Dict, Any, List, Callable
from typing import Union
from urllib.parse import urlencode
from uuid import uuid4

import orjson
import pandas as pd
import requests
import websocket
from pybit.unified_trading import HTTP
from requests import Session, Response, adapters

logger = logging.getLogger(__name__)

logging.getLogger('websocket').setLevel(logging.ERROR)


class InsufficientFunds(Exception):
    pass


class BadRequest(Exception):
    pass


class InvalidOrder(Exception):
    pass


class AuthenticationError(Exception):
    pass


class ExchangeError(Exception):
    pass


class InvalidNonce(Exception):
    pass


class OrderNotFound(Exception):
    pass


class RateLimitExceeded(Exception):
    pass


class PermissionDenied(Exception):
    pass


def _run_with_timeout(func, name: Optional[str] = None, timeout_seconds: float = 10.0, *args, **kwargs) -> Optional[
    Any]:
    result = None
    exc = None
    name = threading.current_thread().name if name is None else name

    def target():
        nonlocal result, exc
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            exc = e

    thread = threading.Thread(target=target, name=name)
    thread.start()
    thread.join(timeout=timeout_seconds)

    if thread.is_alive():
        # noinspection PyBroadException
        try:
            func_name = func.__name__
        except Exception:
            func_name = func
        if len(args) > 0:
            timeout_msg = f'Timeout: {func_name}, {args}, {json.dumps(kwargs, default=str)}.'
        else:
            timeout_msg = f'Timeout: {func_name}, {json.dumps(kwargs, default=str)}.'
        raise TimeoutError(timeout_msg)
    if exc is not None:
        raise exc
    return result


class ExceptionMapper:
    # https://bybit-exchange.github.io/docs/derivativesV3/unified_margin/#110023
    # https://github.com/ccxt/ccxt/blob/master/python/ccxt/bybit.py
    exception_map = {
        '-10009': BadRequest,  # {"ret_code":-10009,"ret_msg":"Invalid period!","result":null,"token":null}
        '-1004': BadRequest,
        # {"ret_code":-1004,"ret_msg":"Missing required parameter \u0027symbol\u0027",
        # "ext_code":null,"ext_info":null,"result":null}
        '-1021': BadRequest,
        # {"ret_code":-1021,"ret_msg":"Timestamp for self request is outside of the
        # recvWindow.","ext_code":null,"ext_info":null,"result":null}
        '-1103': BadRequest,  # An unknown parameter was sent.
        '-1140': InvalidOrder,
        # {"ret_code":-1140,"ret_msg":"Transaction amount lower than the minimum.",
        # "result":{},"ext_code":"","ext_info":null,"time_now":"1659204910.248576"}
        '-1197': InvalidOrder,
        # {"ret_code":-1197,"ret_msg":"Your order quantity to buy is too large. The
        # filled price may deviate significantly from the market price. Please try again",
        # "result":{},"ext_code":"","ext_info":null,"time_now":"1659204531.979680"}
        '-2013': InvalidOrder,
        # {"ret_code":-2013,"ret_msg":"Order does not exist.","ext_code":null,"ext_info":null,"result":null}
        '-2015': AuthenticationError,  # Invalid API-key, IP, or permissions for action.
        '-6017': BadRequest,  # Repayment amount has exceeded the total liability
        '-6025': BadRequest,  # Amount to borrow cannot be lower than the min. amount to borrow(per transaction)
        '-6029': BadRequest,  # Amount to borrow has exceeded the user's estimated max amount to borrow
        '5004': ExchangeError,
        # {"retCode":5004,"retMsg":"Server Timeout","result":null,"retExtInfo":{},"time":1667577060106}
        '7001': BadRequest,  # {"retCode":7001,"retMsg":"request params type error"}
        '10001': BadRequest,  # parameter error
        '10002': InvalidNonce,  # request expired, check your timestamp and recv_window
        '10003': AuthenticationError,  # Invalid apikey
        '10004': AuthenticationError,  # invalid sign
        '10005': PermissionDenied,  # permission denied for current apikey
        '10006': RateLimitExceeded,  # too many requests
        '10007': AuthenticationError,  # api_key not found in your request parameters
        '10010': PermissionDenied,  # request ip mismatch
        '10016': ExchangeError,  # {"retCode":10016,"retMsg":"System error. Please try again later."}
        '10017': BadRequest,  # request path not found or request method is invalid
        '10018': RateLimitExceeded,  # exceed ip rate limit
        '10020': PermissionDenied,
        # {"retCode":10020,"retMsg":"your account is not a unified margin account,
        # please update your account","result":null,"retExtInfo":null,"time":1664783731123}
        '12201': BadRequest,
        # {"retCode":12201,"retMsg":"Invalid orderCategory parameter.","result":{},
        # "retExtInfo":null,"time":1666699391220}
        '131001': InsufficientFunds,
        # {"retCode":131001,"retMsg":"the available balance is not sufficient to cover
        # the handling fee","result":{},"retExtInfo":{},"time":1666892821245}
        '20001': OrderNotFound,  # Order not exists
        '20003': InvalidOrder,  # missing parameter side
        '20004': InvalidOrder,  # invalid parameter side
        '20005': InvalidOrder,  # missing parameter symbol
        '20006': InvalidOrder,  # invalid parameter symbol
        '20007': InvalidOrder,  # missing parameter order_type
        '20008': InvalidOrder,  # invalid parameter order_type
        '20009': InvalidOrder,  # missing parameter qty
        '20010': InvalidOrder,  # qty must be greater than 0
        '20011': InvalidOrder,  # qty must be an integer
        '20012': InvalidOrder,  # qty must be greater than zero and less than 1 million
        '20013': InvalidOrder,  # missing parameter price
        '20014': InvalidOrder,  # price must be greater than 0
        '20015': InvalidOrder,  # missing parameter time_in_force
        '20016': InvalidOrder,  # invalid value for parameter time_in_force
        '20017': InvalidOrder,  # missing parameter order_id
        '20018': InvalidOrder,  # invalid date format
        '20019': InvalidOrder,  # missing parameter stop_px
        '20020': InvalidOrder,  # missing parameter base_price
        '20021': InvalidOrder,  # missing parameter stop_order_id
        '20022': BadRequest,  # missing parameter leverage
        '20023': BadRequest,  # leverage must be a number
        '20031': BadRequest,  # leverage must be greater than zero
        '20070': BadRequest,  # missing parameter margin
        '20071': BadRequest,  # margin must be greater than zero
        '20084': BadRequest,  # order_id or order_link_id is required
        '30001': BadRequest,  # order_link_id is repeated
        '30003': InvalidOrder,  # qty must be more than the minimum allowed
        '30004': InvalidOrder,  # qty must be less than the maximum allowed
        '30005': InvalidOrder,  # price exceeds maximum allowed
        '30007': InvalidOrder,  # price exceeds minimum allowed
        '30008': InvalidOrder,  # invalid order_type
        '30009': ExchangeError,  # no position found
        '30010': InsufficientFunds,  # insufficient wallet balance
        '30011': PermissionDenied,  # operation not allowed as position is undergoing liquidation
        '30012': PermissionDenied,  # operation not allowed as position is undergoing ADL
        '30013': PermissionDenied,  # position is in liq or adl status
        '30014': InvalidOrder,  # invalid closing order, qty should not greater than size
        '30015': InvalidOrder,  # invalid closing order, side should be opposite
        '30016': ExchangeError,  # TS and SL must be cancelled first while closing position
        '30017': InvalidOrder,  # estimated fill price cannot be lower than current Buy liq_price
        '30018': InvalidOrder,  # estimated fill price cannot be higher than current Sell liq_price
        '30019': InvalidOrder,
        # cannot attach TP/SL params for non-zero position when placing non-opening position order
        '30020': InvalidOrder,  # position already has TP/SL params
        '30021': InvalidOrder,  # cannot afford estimated position_margin
        '30022': InvalidOrder,  # estimated buy liq_price cannot be higher than current mark_price
        '30023': InvalidOrder,  # estimated sell liq_price cannot be lower than current mark_price
        '30024': InvalidOrder,  # cannot set TP/SL/TS for zero-position
        '30025': InvalidOrder,  # trigger price should bigger than 10% of last price
        '30026': InvalidOrder,  # price too high
        '30027': InvalidOrder,  # price set for Take profit should be higher than Last Traded Price
        '30028': InvalidOrder,  # price set for Stop loss should be between Liquidation price and Last Traded Price
        '30029': InvalidOrder,  # price set for Stop loss should be between Last Traded Price and Liquidation price
        '30030': InvalidOrder,  # price set for Take profit should be lower than Last Traded Price
        '30031': InsufficientFunds,  # insufficient available balance for order cost
        '30032': InvalidOrder,  # order has been filled or cancelled
        '30033': RateLimitExceeded,  # The number of stop orders exceeds maximum limit allowed
        '30034': OrderNotFound,  # no order found
        '30035': RateLimitExceeded,  # too fast to cancel
        '30036': ExchangeError,  # the expected position value after order execution exceeds the current risk limit
        '30037': InvalidOrder,  # order already cancelled
        '30041': ExchangeError,  # no position found
        '30042': InsufficientFunds,  # insufficient wallet balance
        '30043': InvalidOrder,  # operation not allowed as position is undergoing liquidation
        '30044': InvalidOrder,  # operation not allowed as position is undergoing AD
        '30045': InvalidOrder,  # operation not allowed as position is not normal status
        '30049': InsufficientFunds,  # insufficient available balance
        '30050': ExchangeError,  # any adjustments made will trigger immediate liquidation
        '30051': ExchangeError,  # due to risk limit, cannot adjust leverage
        '30052': ExchangeError,  # leverage can not less than 1
        '30054': ExchangeError,  # position margin is invalid
        '30057': ExchangeError,  # requested quantity of contracts exceeds risk limit
        '30063': ExchangeError,  # reduce-only rule not satisfied
        '30067': InsufficientFunds,  # insufficient available balance
        '30068': ExchangeError,  # exit value must be positive
        '30074': InvalidOrder,
        # can't create the stop order, because you expect the order will be triggered when the
        # LastPrice(or IndexPrice、 MarkPrice, determined by trigger_by) is raising to stop_px,
        # but the LastPrice(or IndexPrice、 MarkPrice) is already equal to or greater than stop_px,
        # please adjust base_price or stop_px
        '30075': InvalidOrder,
        # can't create the stop order, because you expect the order will be triggered when the
        # LastPrice(or IndexPrice、 MarkPrice, determined by trigger_by) is falling to stop_px,
        # but the LastPrice(or IndexPrice、 MarkPrice) is already equal to or less than stop_px,
        # please adjust base_price or stop_px
        '30078': ExchangeError,
        # {"ret_code":30078,"ret_msg":"","ext_code":"","ext_info":"","result":null,"time_now":"1644853040.916000",
        # "rate_limit_status":73,"rate_limit_reset_ms":1644853040912,"rate_limit":75}
        # '30084': BadRequest,  # Isolated not modified, see handleErrors below
        '33004': AuthenticationError,  # apikey already expired
        '34026': ExchangeError,  # the limit is no change
        '34036': BadRequest,
        # {"ret_code":34036,"ret_msg":"leverage not modified","ext_code":"","ext_info":"","result":null,
        # "time_now":"1652376449.258918","rate_limit_status":74,"rate_limit_reset_ms":1652376449255,"rate_limit":75}
        '35015': BadRequest,
        # {"ret_code":35015,"ret_msg":"Qty not in range","ext_code":"","ext_info":"","result":null,"time_now":
        # "1652277215.821362","rate_limit_status":99,"rate_limit_reset_ms":1652277215819,"rate_limit":100}
        '130006': InvalidOrder,
        # {"ret_code":130006,"ret_msg":"The number of contracts exceeds maximum limit allowed: too large",
        # "ext_code":"","ext_info":"","result":null,"time_now":"1658397095.099030","rate_limit_status":99,
        # "rate_limit_reset_ms":1658397095097,"rate_limit":100}
        '130021': InsufficientFunds,
        '110007': InsufficientFunds,
        # {"ret_code":130021,"ret_msg":"orderfix price failed for CannotAffordOrderCost.","ext_code":"",
        # "ext_info":"","result":null,"time_now":"1644588250.204878","rate_limit_status":98,
        # "rate_limit_reset_ms":1644588250200,"rate_limit":100} |  {"ret_code":130021,"ret_msg":
        # "oc_diff[1707966351], new_oc[1707966351] with ob[....]+AB[....]","ext_code":"","ext_info":"",
        # "result":null,"time_now":"1658395300.872766","rate_limit_status":99,"rate_limit_reset_ms":1658395300855,
        # "rate_limit":100} caused issues/9149#issuecomment-1146559498
        '130074': InvalidOrder,
        # {"ret_code":130074,"ret_msg":"expect Rising, but trigger_price[190000000]
        # \u003c= current[211280000]??LastPrice","ext_code":"","ext_info":"","result":null,
        # "time_now":"1655386638.067076","rate_limit_status":97,"rate_limit_reset_ms":1655386638065,
        # "rate_limit":100}
        '3100116': BadRequest,
        # {"retCode":3100116,"retMsg":"Order quantity below the lower limit 0.01.","result":null,
        # "retExtMap":{"key0":"0.01"}}
        '3100198': BadRequest,  # {"retCode":3100198,"retMsg":"orderLinkId can not be empty.",
        # "result":null,"retExtMap":{}}
        '3200300': InsufficientFunds,
        # {"retCode":3200300,"retMsg":"Insufficient margin balance.","result":null,"retExtMap":{}}
        '110001': OrderNotFound,  # Order does not exist
        '110003': InvalidOrder,  # Order price is out of permissible range
        '110004': InsufficientFunds,  # Insufficient wallet balance
        '110005': ExchangeError,  # position status.
        '110006': InvalidOrder,  # cannot afford estimated position_margin
        '110008': InvalidOrder,  # Order has been finished or canceled
        '110009': RateLimitExceeded,  # The number of stop orders exceeds maximum limit allowed
        '110010': InvalidOrder,  # Order already cancelled
        '110011': PermissionDenied,  # Any adjustments made will trigger immediate liquidation
        '110012': InsufficientFunds,  # Available balance not enough,
        '110013': ExchangeError,  # Due to risk limit, cannot set leverage
        '110014': InsufficientFunds,  # Available balance not enough to add margin
        '110015': ExchangeError,  # the position is in cross_margin
        '110016': ExchangeError,  # Requested quantity of contracts exceeds risk limit,
        # please adjust your risk limit level before trying again
        '110017': InvalidOrder,  # Reduce-only rule not satisfied
        '110018': BadRequest,  # userId illegal
        '110019': BadRequest,  # orderId illegal
        '110020': InvalidOrder,  # number of active orders greater than 500
        '110021': BadRequest,  # Open Interest exceeded
        '110022': InvalidOrder,  # qty has been limited, cannot modify the order to add qty
        '110023': BadRequest,
        '110024': BadRequest,
        '110025': ExchangeError,
        '110026': ExchangeError,
        '110027': ExchangeError,
        '110028': ExchangeError,
        '110029': ExchangeError,
        '110030': InvalidOrder,
        '110031': ExchangeError,
        '110032': ExchangeError,
        '110033': ExchangeError,
        '110034': ExchangeError,
        '110035': ExchangeError,
        '110036': ExchangeError,
        '110037': ExchangeError,
        '110038': ExchangeError,
        '110039': ExchangeError,
        '110040': ExchangeError,
        '110041': ExchangeError,
        '110042': ExchangeError,
        '110043': ExchangeError,
        '110044': InsufficientFunds,  # Insufficient available margin
        '110045': InsufficientFunds,  # Insufficient wallet balance
        '110046': ExchangeError,
        '110047': InsufficientFunds,  # Risk limit cannot be adjusted due to insufficient available margin
        '110048': ExchangeError,
        '110049': ExchangeError,
        '110050': ExchangeError,
        '110051': InsufficientFunds,
        '110052': InsufficientFunds,
        '110053': InsufficientFunds,
        '110054': ExchangeError,
        '110055': ExchangeError,
        '110056': ExchangeError,
        '110057': ExchangeError,
        '110058': ExchangeError,
        '110059': ExchangeError,
        '110060': ExchangeError,
        '110061': ExchangeError,
        '110062': ExchangeError,
        '110063': ExchangeError,
        '110064': ExchangeError,
        '110065': ExchangeError,
        '110066': ExchangeError,
        '110067': ExchangeError,
        '110068': ExchangeError,
        '110069': ExchangeError,
        '110070': ExchangeError,
        '3400214': ExchangeError,
    }

    @staticmethod
    def from_code(code: int):
        return ExceptionMapper.exception_map.get(str(code))


def download_public_trades(
        start_date: datetime,
        end_date: datetime,
        symbol: str,
        exchange: str = 'bybit'
) -> pd.DataFrame:
    is_bybit = exchange == 'bybit'
    assert exchange in ['bybit', 'binance']
    assert symbol.endswith('USDT')
    tmp = Path(tempfile.gettempdir()) / f'{exchange}_trades'
    tmp.mkdir(parents=True, exist_ok=True)
    cursor = start_date
    records = []
    while cursor <= end_date.replace(hour=23, minute=59, second=59, microsecond=999999):
        date = cursor.strftime('%Y-%m-%d')
        if is_bybit:
            filename = f'{symbol}{date}.csv.gz'
        else:
            filename = f'{symbol}-aggTrades-{date}.zip'
        local_file = tmp / filename  # (tmp / Path(filename).stem).with_suffix('.csv')
        if not local_file.exists():
            if is_bybit:
                url = f'https://public.bybit.com/trading/{symbol}/{filename}'
            else:
                url = f'https://data.binance.vision/data/futures/um/daily/aggTrades/{symbol}/{filename}'
            print(f'Downloading {url} to {local_file}.')
            compression = 'gzip' if is_bybit else 'zip'
            # noinspection PyTypeChecker
            data = pd.read_csv(url, compression=compression)
            # noinspection PyTypeChecker
            data.to_csv(local_file, compression=compression, index=False)
        else:
            print(f'Reading from {local_file}.')
            data = pd.read_csv(local_file)
        records.append(data)
        cursor += timedelta(days=1)
    records = pd.concat(records, axis=0)
    if is_bybit:
        records['side'] = records['side'].str.lower()
        records['dateTime'] = pd.to_datetime(records['timestamp'], unit='s')
    else:
        records['dateTime'] = pd.to_datetime(records['transact_time'], unit='ms')
        records['side'] = records['is_buyer_maker'].apply(lambda x: 'sell' if x else 'buy')
    records.set_index('dateTime', inplace=True)
    records = records[start_date:end_date]
    return records


def _convert_ts_to_datetime(ts: float) -> datetime:
    num_digits = len(str(int(ts)))
    # time.time() has 10 digits.
    return datetime.utcfromtimestamp(ts / 10 ** (num_digits - 10))


def _convert_ts(ts: float) -> float:
    num_digits = len(str(int(ts)))
    # time.time() has 10 digits.
    return ts / 10 ** (num_digits - 10)


# https://bybit-exchange.github.io/docs/derivativesV3/unified_margin

def _result_to_float_values(d: Union[List, dict]) -> Union[List, dict, List[float]]:
    res = {}
    if isinstance(d, list):
        return [_result_to_float_values(a) for a in d]
    for k, v in d.items():
        if isinstance(v, list):
            res[k] = [_result_to_float_values(a) for a in v]
        elif isinstance(v, dict):
            res[k] = _result_to_float_values(v)
        else:
            try:
                if v is None or k in {'orderId', 'orderLinkId'}:
                    res[k] = v
                else:
                    res[k] = float(v)
            except ValueError:
                res[k] = v
    return res


class BatchRequest:

    def __init__(self, every: float, category: str, func: Callable):
        # https://bybit-exchange.github.io/docs/v5/order/batch-place
        # https://bybit-exchange.github.io/docs/v5/order/batch-amend
        self._push_queue = queue.Queue()
        self._results = {}
        self._func = func
        self._every = every
        self._category = category
        self._init = False
        self._lock = threading.Lock()
        # Bybit rate limit.
        self._max_per_request = 10 if category == 'linear' else 20
        self._throttler = ThrottlerFast(self._max_per_request + 1, 1, 'batch_request')

    def init(self):
        _runs_in_a_thread(self.run, name='BatchRequest')

    def run(self):
        self._init = True
        while True:
            try:
                batch, keys = self.get_next_batch()
                if len(batch) > 0:
                    # protected with timeout.
                    for _ in range(len(batch)):
                        self._throttler.submit()
                    # Too many visits. Exceeded the API Rate Limit.
                    batch_request = {'category': self._category, 'request': batch}
                    logger.info(f'Batch request: ({self._func.__name__}): {json.dumps(batch_request)}.')
                    results = self._func(batch_request)
                    logger.info(f'Batch response: ({self._func.__name__}): {json.dumps(results)}.')
                    self.process_response(keys, results)
            except Exception as e:
                logger.warning(f'BatchRequest: {str(e)}.')
            finally:
                time.sleep(0.001)

    def send_request(self, param: Dict) -> str:
        with self._lock:
            if not self._init:
                self.init()
        key = str(uuid4())
        self._push_queue.put([key, param])
        return key

    def check_result(self, key: str, timeout: float):
        end_time = time.time() + timeout
        while time.time() < end_time:
            result = self._results.get(key)
            if result is not None:
                del self._results[key]
                return result
            time.sleep(0.001)
        raise TimeoutError(f'Batch request timeout ({timeout}s) for {key}.')

    def process_response(self, keys: List[str], results: Dict):
        num_results = len(results['result']['list'])
        for i in range(num_results):
            status = results['retExtInfo']['list'][i]
            resp = results['result']['list'][i]
            resp['retCode'] = status['code']
            resp['retMsg'] = status['msg']
            self._results[keys[i]] = resp

    def get_next_batch(self):
        batch = []
        keys = []
        while True:
            try:
                req = self._push_queue.get_nowait()
            except queue.Empty:
                req = None
            if req is not None:
                key, req_ = req
                item_ = {a: b for a, b in req_.items() if b is not None}
                # integer, float -> string. Bybit is weird I know.
                item_ = {key: str(value) if not isinstance(value, bool) else value for key, value in
                         item_.items()}
                batch.append(item_)
                keys.append(key)
                if len(batch) >= self._max_per_request:
                    break
            else:
                break
        return batch, keys


class ThrottlerFast:
    delays = deque(maxlen=1000)

    def __init__(self, max_num_requests=30, every=3, name=''):
        # rules: global. https://www.kucoin.com/news/en-adjustment-of-the-spot-and-futures-api-request-limit.
        # - global: no more than 30 orders per 3s.
        self.max_num_requests = max_num_requests
        self.key = '[throttle]' + f'[{name}]' if len(name) > 0 else ''
        self.every = every
        self.times = deque(maxlen=self.max_num_requests + 1)
        self.lock = threading.Lock()

    def check_rule_and_wait(self, times, max_num_requests, every):
        now = time.time()
        cutoff_time = now - every
        orders = [a for a in times if a >= cutoff_time]
        if len(orders) >= max(max_num_requests - 1, 1):  # -1 to be safe.
            # min time to wait for an order to be pushed out of the buffer.
            time_to_wait = min([o - cutoff_time + 0.01 for o in orders])
            logger.info(f'{self.key} Wait {time_to_wait:.3f} seconds.')
            assert time_to_wait >= 0
            time.sleep(time_to_wait)

    def _execute(self):
        with self.lock:
            self.check_rule_and_wait(self.times, max_num_requests=self.max_num_requests, every=self.every)
        self.times.append(time.time())

    def submit(self, log_entry: bool = False):
        req_time = time.time()
        log = logger.info if log_entry else logger.debug
        log(f'{self.key} Submit request.')
        self._execute()
        resp_time = time.time()
        delay = resp_time - req_time
        if delay > 0.2:
            # fills -> we don't really care to see it in the logs. The API is so slow for them.
            log_func = logger.warning if self.key != '[throttle][fills]' else logger.debug
            log_func(f'{self.key} Small delay: {int(delay * 1000)} ms.')
            self.delays.append({'time': time.time(), 'delay': delay})
        log(f'{self.key} Response received.')


def _runs_in_a_thread(func, name=None, args=()):
    if name is None:
        name = str(func)
    logger.info(f'New thread started: {name}.')
    t = threading.Thread(target=func, daemon=True, name=name, args=args)
    t.start()
    return t


def _read_credentials(credentials_file: Path):
    with open(credentials_file.expanduser()) as r:
        credentials = json.load(r)
    return credentials


def _mid_price(bid: float, ask: float) -> float:
    return round(bid * 0.5 + ask * 0.5, 8)


def _round_tick(x, tick_size):
    return round(x / tick_size) * tick_size


ACCOUNT_TYPE_UNIFIED = 'UNIFIED'
ACCOUNT_TYPE_CONTRACT = 'CONTRACT'
ACCOUNT_TYPE_COPY_TRADING = 'COPYTRADING'


class ByBit:

    def __init__(
            self,
            credentials: Optional[Union[Path, str]] = None,
            minimal: bool = False,
            subscribe_to_order_books: bool = False,
            subscribe_to_tickers: bool = False,
            subscribe_to_private_feed: bool = True,
            auto_pull_rest_tickers: bool = True,
            orderbook_depth: int = 50,
            category: str = 'linear',
            base_url: str = 'https://api.bybit.com',
            timeout: int = 3,
            retries: int = 1
    ):
        if minimal:
            subscribe_to_order_books = False
            subscribe_to_tickers = False
            subscribe_to_private_feed = False
            auto_pull_rest_tickers = False
        self.credentials = credentials
        self.rest = ByBitRest.from_credentials_file(
            self.credentials, category=category,
            base_url=base_url, timeout=timeout,
            retries=retries
        )
        self.subscribe_to_order_books = subscribe_to_order_books
        self.subscribe_to_tickers = subscribe_to_tickers
        self.orderbook_depth = orderbook_depth
        self.public_feed = None
        if self.subscribe_to_order_books or self.subscribe_to_tickers:
            self.public_feed = ByBitStream(
                credentials=None, category=category,
                subscribe_to_order_books=self.subscribe_to_order_books,
                subscribe_to_tickers=self.subscribe_to_tickers,
                orderbook_depth=self.orderbook_depth,
                private=False, background=True,
            )
        self.private_feed = None
        if subscribe_to_private_feed and self.credentials is not None:
            self.private_feed = ByBitStream(
                self.credentials, private=True, background=True
            )
        self._rest_tickers = self.fetch_rest_tickers()
        self.rest_tickers_last_time = time.time()
        if auto_pull_rest_tickers:
            _runs_in_a_thread(self._run_fetch_rest_tickers_forever, name='RestTicker')

    def fetch_rest_tickers(self) -> Dict[str, Dict]:
        return {m['symbol']: m for m in self.rest.get_markets()}

    def _run_fetch_rest_tickers_forever(self):
        while True:
            try:
                self._rest_tickers = self.fetch_rest_tickers()
                self.rest_tickers_last_time = time.time()
            except Exception as e:
                logger.warning(f'_run_fetch_rest_tickers_forever: {str(e)}.')
            finally:
                time.sleep(1.0)

    def get_positions(self, symbol: Optional[str] = None, use_rest: bool = True, **kwargs) -> List[Dict]:
        # Dangerous function if used with websocket.

        def rest():
            return self.rest.get_positions(**kwargs)

        if self.private_feed is not None and not use_rest:
            results = list(self.private_feed.position_handler.get_positions().values())
        else:
            results = rest()
        if symbol is not None:
            results = [a for a in results if a['symbol'] == symbol]
        return results

    def get_orderbook(self, symbol: str, depth: Optional[int] = None) -> dict:
        if self.subscribe_to_order_books:
            self.public_feed.subscribe_to_orderbook(symbol, depth)
            # depth is defined in the stream.
            ob = self.public_feed.orderbook_handler.get_orderbook(symbol)
            if ob is not None:
                return ob
        return self.rest.get_orderbook(symbol, depth)

    def get_orders(
            self,
            symbol: Optional[str] = None,
            order_id: Optional[str] = None,
            client_id: Optional[str] = None,
            **kwargs
    ) -> List[dict]:
        return self.rest.get_orders(symbol=symbol, order_id=order_id, client_id=client_id, **kwargs)

    def get_open_orders(self, symbol: Optional[str] = None, **kwargs) -> List[dict]:
        return self.rest.get_open_orders(symbol, **kwargs)

    def get_ticker(self, symbol: str) -> Dict:
        return self.get_tickers(symbol)

    def get_tickers(self, symbol: Optional[str] = None, force_use_rest: bool = False):
        if self.subscribe_to_tickers and not force_use_rest:
            if symbol is not None:
                self.public_feed.subscribe_to_ticker(symbol)
                return self.public_feed.ticker_handler.get_ticker(symbol)
            else:
                return self.public_feed.ticker_handler.get_tickers()
        else:  # REST.
            if symbol is not None:
                return self._rest_tickers[symbol]
            return self._rest_tickers

    def get_order_status(
            self,
            order_id: Optional[str] = None,
            client_id: Optional[str] = None,
            **kwargs
    ) -> Optional[dict]:
        if self.private_feed is not None:
            return self.private_feed.order_status_handler.get_order_status(order_id=order_id, client_id=client_id)
        results = self.rest.get_orders(order_id=order_id, client_id=client_id, **kwargs)
        return results[0] if len(results) > 0 else None

    def modify_order(
            self,
            symbol: Optional[str] = None,
            order_id: Optional[str] = None,
            client_id: Optional[str] = None,
            size: Optional[float] = None,
            price: Optional[float] = None,
            batch: bool = False,
            **kwargs
    ) -> dict:
        return self.rest.modify_order(
            symbol=symbol, order_id=order_id, client_id=client_id, size=size, price=price, batch=batch, **kwargs
        )

    # noinspection PyShadowingBuiltins
    def place_order(
            self,
            symbol: str,
            side: str,
            size: float,
            price: Optional[float] = None,
            type: str = 'limit',
            reduce_only: bool = False,
            ioc: bool = False,
            post_only: bool = False,
            client_id: Optional[str] = None,
            batch: bool = False,
            **kwargs
    ) -> dict:
        return self.rest.place_order(
            symbol=symbol, side=side, price=price, size=size, type=type, reduce_only=reduce_only,
            ioc=ioc, post_only=post_only, client_id=client_id, batch=batch, **kwargs
        )

    def cancel_order(
            self,
            order_id: Optional[str] = None,
            client_id: Optional[str] = None,
            symbol: Optional[str] = None,
            **kwargs
    ):
        return self.rest.cancel_order(order_id=order_id, client_id=client_id, symbol=symbol, **kwargs)

    def cancel_all_orders(
            self,
            symbol: Optional[str] = None,
            **kwargs
    ) -> List[dict]:
        return self.rest.cancel_all_orders(
            symbol=symbol, **kwargs
        )

    def get_balances(self, **kwargs):
        return self.rest.get_balances(**kwargs)

    def get_executions(
            self,
            symbol: Optional[str] = None,
            order_id: Optional[str] = None,
            client_id: Optional[str] = None,
            use_websocket: bool = True,
            **kwargs
    ) -> List[dict]:
        if not use_websocket:
            return self.rest.get_executions(symbol, order_id, client_id, **kwargs)
        elif self.private_feed is not None:
            return self.private_feed.execution_handler.get_executions(symbol, order_id, client_id)
        return self.rest.get_executions(symbol, order_id, client_id, **kwargs)

    def get_private_funding_history(
            self,
            symbol: str,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
            **kwargs
    ) -> pd.DataFrame:
        trades = self.get_trade_history(start_date=start_date, end_date=end_date, **kwargs)
        funding = pd.DataFrame([a for a in trades if a['funding'] != '' and a['symbol'] == symbol])
        if len(funding) >= 0:
            funding.set_index(funding['transactionTime'].apply(_convert_ts_to_datetime), inplace=True)
            funding.sort_index(inplace=True)
        return funding

    def get_order_history(
            self,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
            **kwargs
    ) -> List[dict]:
        if end_date is None:
            end_date = datetime.utcnow()
        if start_date is None:
            start_date = datetime.utcnow() - timedelta(days=7)
        start_time = int(start_date.replace(tzinfo=timezone.utc).replace(tzinfo=timezone.utc).timestamp() * 1e3)
        end_time = int(end_date.replace(tzinfo=timezone.utc).replace(tzinfo=timezone.utc).timestamp() * 1e3)
        return self.rest.get_order_history(startTime=start_time, endTime=end_time, **kwargs)

    def get_trade_history(
            self,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
            **kwargs
    ) -> List[dict]:
        if end_date is None:
            end_date = datetime.utcnow()
        if start_date is None:
            start_date = datetime.utcnow() - timedelta(days=7)
        start_time = int(start_date.replace(tzinfo=timezone.utc).replace(tzinfo=timezone.utc).timestamp() * 1e3)
        end_time = int(end_date.replace(tzinfo=timezone.utc).replace(tzinfo=timezone.utc).timestamp() * 1e3)
        trades = self.rest.get_trade_history(startTime=start_time, endTime=end_time, **kwargs)
        return [t for t in trades if t['type'] == 'TRADE']

    def get_funding_history(
            self,
            symbol: str,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
            **kwargs
    ) -> pd.DataFrame:
        if start_date is None:
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=7)
        start_time = int(start_date.replace(tzinfo=timezone.utc).replace(tzinfo=timezone.utc).timestamp() * 1e3)
        end_time = int(end_date.replace(tzinfo=timezone.utc).replace(tzinfo=timezone.utc).timestamp() * 1e3)
        history = pd.DataFrame(
            self.rest.get_funding_history(symbol=symbol, startTime=start_time, endTime=end_time, **kwargs)
        )
        if len(history) >= 0:
            history.set_index(history['fundingRateTimestamp'].apply(_convert_ts_to_datetime), inplace=True)
            history['fundingRatePct'] = history['fundingRate'] * 100
            history.drop(['fundingRateTimestamp', 'fundingRate'], inplace=True, axis=1)
            history.sort_index(inplace=True)
        return history


class TimeInForce(Enum):
    GTC = 'GTC'
    IOC = 'IOC'
    FOK = 'FOK'
    POS = 'PostOnly'

    def __str__(self):
        return self.value


class OrderFilter(Enum):
    ORD = 'Order'
    STOP = 'StopOrder'

    def __str__(self):
        return self.value


class OrderStatus(Enum):
    # https://bybit-exchange.github.io/docs/derivativesV3/unified_margin/#order-order
    CREATED = 'Created'
    NEW = 'New'
    REJECTED = 'Rejected'
    PARTIALLY_FILLED = 'PartiallyFilled'
    FILLED = 'Filled'
    PENDING_CANCEL = 'PendingCancel'
    CANCELLED = 'Cancelled'

    # Only for conditional orders
    UN_TRIGGERED = 'Untriggered'
    DEACTIVATED = 'Deactivated'
    TRIGGERED = 'Triggered'
    ACTIVE = 'Active'

    def __str__(self):
        return self.value


class ByBitExecutions:
    def __init__(self):
        self.executions = {}

    def get_executions(
            self,
            symbol: Optional[str] = None,
            order_id: Optional[str] = None,
            client_id: Optional[str] = None
    ) -> List[dict]:
        if symbol is not None:
            executions = self.executions.get(symbol)
            if executions is None:
                executions = []
        else:
            executions = sum(list(self.executions.values()), [])
        if order_id is not None:
            executions = [e for e in executions if e['orderId'] == order_id]
        if client_id is not None:
            executions = [e for e in executions if e['orderLinkId'] == client_id]
        return executions

    def on_message(self, msg: dict):
        data = msg['data']
        data = _result_to_float_values(data)
        if 'result' in data:
            data = data['result']
        for result in data:
            # {'symbol': 'ADAUSDT', 'side': 'Sell', 'orderId': 'a2c60728-8f5c-46e0-920b-e32952014448',
            # 'execId': '44aed711-5a9e-5d8c-bcdf-21ec3746065b', 'orderLinkId': '',
            # 'execPrice': '0.33020000', 'orderQty': '1.0000', 'execType': 'TRADE',
            # 'execQty': '1.0000', 'leavesQty': '0.0000', 'execFee': '0.00019812',
            # 'execTime': 1668919809762, 'feeRate': '0.000600', 'execValue': '0.33020000',
            # 'lastLiquidityInd': 'TAKER', 'orderPrice': '0.31370000', 'orderType': 'Market',
            # 'stopOrderType': 'UNKNOWN', 'blockTradeId': ''}
            symbol = result['symbol']
            if symbol not in self.executions:
                self.executions[symbol] = []
            self.executions[symbol].append(result)
            order_id = result['orderId']
            short_order_id = order_id.split('-')[-1]
            logger.info(f'Execution: {short_order_id} {result["execType"].title()} {result["side"]} '
                        f'{result["symbol"]} {result["execQty"]}@{result["execPrice"]}.')


class ByBitRealTimePositions:
    def __init__(self):
        self.positions = {}

    def get_positions(self):
        return {a: b for a, b in self.positions.items() if b['size'] != 0.0}

    def on_message(self, msg: dict):
        if self.positions is None:
            self.positions = {}  # init.
        data = msg['data']
        data = _result_to_float_values(data)
        if 'result' in data:
            data = data['result']
        for result in data:
            # {'positionIdx': 0, 'riskId': 1, 'symbol': 'BTCUSDT', 'side': 'None', 'size': '0.0000',
            # 'entryPrice': '0.00000000', 'leverage': '10', 'markPrice': '16851.50000000',
            # 'positionIM': '0.00000000', 'positionMM': '0.00000000', 'takeProfit': '',
            # 'stopLoss': '', 'trailingStop': '', 'positionValue': '0.00000000',
            # 'unrealisedPnl': '0.00000000', 'cumRealisedPnl': '0.00000000',
            # 'createdTime': 1668743829404, 'updatedTime': 1668919315146, '
            # tpslMode': 'Full', 'sessionAvgPrice': ''}
            self.positions[result['symbol']] = result


class ByBitRealTimeOrderStatuses:
    def __init__(self):
        self.order_statuses_order_id = {}
        self.order_statuses_order_client_id = {}

    def get_order_status(self, order_id: Optional[str] = None, client_id: Optional[str] = None) -> Optional[dict]:
        if order_id is not None:
            return self.order_statuses_order_id.get(order_id)
        if client_id is not None:
            return self.order_statuses_order_client_id.get(client_id)

    def on_message(self, msg: dict):
        data = msg['data']
        data = _result_to_float_values(data)
        if 'result' in data:
            data = data['result']
        for result in data:
            # {'orderId': 'e2b14fef-0332-4240-a884-8000cdae0c1e', 'orderLinkId': '', 'symbol': 'BTCUSDT',
            # 'side': 'Buy', 'orderType': 'Limit', 'price': '10000.00000000', 'qty': '0.0100',
            # 'timeInForce': 'PostOnly', 'orderStatus': 'New', 'cumExecQty': '0.0000',
            # 'cumExecValue': '0.00000000', 'cumExecFee': '0.00000000', 'stopOrderType': 'UNKNOWN',
            # 'triggerBy': 'UNKNOWN', 'triggerPrice': '', 'reduceOnly': False, 'closeOnTrigger': False,
            # 'createdTime': 1668919315140, 'updatedTime': 1668919995200, 'iv': '', 'orderIM': '',
            # 'takeProfit': '', 'stopLoss': '', 'tpTriggerBy': 'UNKNOWN', 'slTriggerBy': 'UNKNOWN',
            # 'basePrice': '', 'blockTradeId': '', 'leavesQty': '0.0100'}
            order_id = result['orderId']
            client_id = result['orderLinkId']
            if order_id is not None and order_id != '':
                self.order_statuses_order_id[order_id] = result
            if client_id is not None and client_id != '':
                self.order_statuses_order_client_id[client_id] = result
            price = '<market>' if result['orderType'] == 'Market' else float(result["price"])
            logger.info(f'OrdStatus: {order_id[-8:]} {result["orderStatus"].lower()} '
                        f'{result["side"].lower()} {result["symbol"]} '
                        f'{float(result["qty"])}@{price}, '
                        f'ReduceOnly={1 if result["reduceOnly"] else 0}, '
                        f'tif={result["timeInForce"]}.')


class ByBitRealTimeTickers:

    def __init__(self):
        self.tickers = {}

    def on_message(self, msg: dict):
        data = msg['data']
        symbol = data['symbol']
        data = _result_to_float_values(data)  # fast enough. 400ms per 100K.
        data['ts_us'] = time.time()
        data['ts'] = _convert_ts(msg['ts'])
        if symbol not in self.tickers or msg['type'] == 'snapshot':
            self.tickers[symbol] = data
        else:
            self.tickers[symbol].update(data)

    def get_tickers(self):
        return dict(self.tickers)

    def get_ticker(self, symbol: str, wait: float = 10.0) -> Dict:
        end = time.time() + wait
        while time.time() < end:
            tickers = self.get_tickers()
            if symbol in tickers:
                return tickers[symbol]
            time.sleep(0.1)

    def get_mid(self, symbol: str) -> Optional[float]:
        if symbol in self.tickers:
            market = self.tickers[symbol]
            return _mid_price(float(market['bid1Price']), float(market['ask1Price']))
        return None


class ByBitOrderBooks:

    def __init__(self):
        self.books = {}

    def get_orderbook(self, symbol: str) -> Optional[dict]:
        if symbol not in self.books:
            return None
        order_book = self.books[symbol].copy()
        bids = order_book['bids']
        asks = order_book['asks']
        bid_prices = list(reversed(sorted(bids)))
        ask_prices = sorted(asks)
        return {
            's': symbol,
            'bids': [[p, bids[p]] for p in bid_prices],
            'asks': [[p, asks[p]] for p in ask_prices],
            'ts': order_book['ts']
        }

    def on_message(self, msg: dict):
        assert msg['type'] in ['snapshot', 'delta']
        snapshot = msg['type'] == 'snapshot'
        timestamp = msg['ts']
        data = msg['data']
        symbol = data['s']
        if symbol not in self.books or snapshot:
            self.books[symbol] = {
                'bids': {float(t[0]): float(t[1]) for t in data['b']},
                'asks': {float(t[0]): float(t[1]) for t in data['a']},
            }
        else:
            keys = [['b', 'bids'], ['a', 'asks']]
            for key in keys:
                for u in data[key[0]]:
                    value = float(u[1])
                    price = float(u[0])
                    if value == 0:
                        del self.books[symbol][key[1]][price]
                    else:
                        self.books[symbol][key[1]][price] = value
        self.books[symbol]['ts'] = timestamp  # datetime.utcfromtimestamp(timestamp / 1e3)
        self.books[symbol]['update_id'] = data['u']


class ByBitRest:

    def __init__(
            self,
            base_url: str = "https://api.bybit.com",
            api_key: Optional[str] = None,
            api_secret: Optional[str] = None,
            timeout: int = 2,
            retries: int = 1,
            category: str = 'linear'
    ) -> None:
        self.timeout = timeout
        self.retries = retries
        self.category = category
        self._session = Session()
        self._session.mount('https://', adapters.HTTPAdapter(pool_maxsize=20))
        self._session.mount('http://', adapters.HTTPAdapter(pool_maxsize=20))
        self._base_url = base_url
        if api_key is None:
            api_key = ''
        if api_secret is None:
            api_secret = ''
        self._api_key = api_key
        self._api_secret = api_secret
        self._recv_window = str(5000)
        self.throttler_order_create = ThrottlerFast(10, 1, 'order_create')
        self.throttler_batch_order_create = ThrottlerFast(10, 1, 'batch_order_create')
        self.throttler_order_amend = ThrottlerFast(10, 1, 'order_amend')
        self.throttler_batch_order_amend = ThrottlerFast(10, 1, 'batch_order_amend')
        self.throttler_order_cancel = ThrottlerFast(10, 1, 'order_cancel')
        self.throttler_order_all_cancel = ThrottlerFast(10, 1, 'order_all_cancel')
        self.throttler_order_realtime = ThrottlerFast(50, 1, 'order_realtime')
        self.throttler_positions_list = ThrottlerFast(50, 1, 'position_list')
        self.throttler_executions_list = ThrottlerFast(50, 1, 'execution_list')
        self.throttler_closed_pnl = ThrottlerFast(50, 1, 'closed_pnl')
        self.throttler_wallet_balance = ThrottlerFast(50, 1, 'wallet_balance')
        self.throttler_global = ThrottlerFast(120, 5, 'global')
        self.throttler_transaction_log = ThrottlerFast(50, 1, 'transaction_log')
        self.throttler_order_history = ThrottlerFast(50, 1, 'order_history')
        self.symbols = self.get_instruments_info()
        if self.category != 'spot':
            self.step_sizes = {s['symbol']: float(s['lotSizeFilter']['qtyStep']) for s in self.symbols}
        else:
            self.step_sizes = {s['symbol']: float(s['lotSizeFilter']['basePrecision']) for s in self.symbols}
        self.min_quantities = {s['symbol']: float(s['lotSizeFilter']['minOrderQty']) for s in self.symbols}
        # https://bybit-exchange.github.io/docs/derivativesV3/unified_margin/#t-ipratelimits
        self.tick_sizes = {s['symbol']: float(s['priceFilter']['tickSize']) for s in self.symbols}
        # Why? Modify requires the symbol, but we can cache it during place_order.
        self._cache_order_id_to_symbols = {}
        self._cache_client_id_to_symbols = {}
        self.batch_amend = BatchRequest(every=1, category=self.category, func=self.batch_modify_order)
        self.batch_create = BatchRequest(every=1, category=self.category, func=self.batch_place_order)
        # for batch requests. I could not figure out why it was not working with the current bitpy implementation.
        self.http_session = HTTP(testnet=False, api_key=api_key, api_secret=api_secret)

    @classmethod
    def from_credentials_file(cls, credentials: Union[Path, str, None] = None, **kwargs):
        api_key = None
        api_secret = None
        if credentials is not None:
            credentials = _read_credentials(Path(credentials).expanduser())
            api_key = credentials['apiKey']
            api_secret = credentials['secret']
        return cls(api_key=api_key, api_secret=api_secret, **kwargs)

    @staticmethod
    def _post_processing(resp: Any, pagination: bool = False) -> Any:
        result = resp
        if not pagination and 'list' in resp:
            result = resp['list']
        if isinstance(result, list):
            result = [_result_to_float_values(r) for r in result]
            if len(result) == 1:
                result = result[0]
        elif isinstance(result, dict):
            result = _result_to_float_values(result)
        return result

    @staticmethod
    def _retry_on_error(
            call, throttler: Optional[ThrottlerFast] = None,
            retries: int = 3, wait=0.0, verbose=True, *args, **kwargs
    ):
        for i in range(retries):
            try:
                if throttler is not None:
                    throttler.submit()
                return call(*args, **kwargs)
            except Exception as e:
                if retries == 1:
                    raise e
                else:
                    if verbose:
                        logger.warning(f'retries: {call.__name__}, args: {args}, kwargs: {kwargs}. Error: {str(e)}.')
                    if i == retries - 1:
                        raise e
                    time.sleep(wait)

    def _get(self, path: str, throttler: Optional[ThrottlerFast] = None,
             params: Optional[Dict[str, Any]] = None, pagination: bool = False, post_processing: bool = True) -> Any:
        return self._req(
            path=path, method='GET', throttler=throttler, params=params,
            pagination=pagination, post_processing=post_processing
        )

    def _post(self, path: str, throttler: Optional[ThrottlerFast] = None,
              params: Optional[Dict[str, Any]] = None, pagination: bool = False, post_processing: bool = True) -> Any:
        return self._req(
            path=path, method='POST', throttler=throttler, params=params,
            pagination=pagination, post_processing=post_processing
        )

    def _req(self, path: str, method: str, throttler: Optional[ThrottlerFast] = None,
             params: Optional[Dict[str, Any]] = None, pagination: bool = False, post_processing: bool = True) -> Any:
        req = self._retry_on_error(
            self._request, retries=self.retries, throttler=throttler, method=method, path=path, params=params
        )
        if post_processing:
            return self._post_processing(req, pagination=pagination)
        return req

    def _sign_request(self, timestamp: str, params: str) -> str:
        payload = str(params)
        param_str = str(timestamp) + self._api_key + self._recv_window + payload
        hash_ = hmac.new(bytes(self._api_secret, "utf-8"), param_str.encode("utf-8"), hashlib.sha256)
        signature = hash_.hexdigest()
        return signature

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]]) -> Any:
        if isinstance(params, dict):
            if len(params) > 0:  # at least one key.
                params = {k: v for k, v in params.items() if v is not None}
                params = {k: v.value if isinstance(v, Enum) else v for k, v in params.items()}
                if method == 'GET':
                    params = urlencode(params)
                else:  # POST
                    params = {k: str(v).lower() if isinstance(v, bool) else str(v) for k, v in params.items()}
                    params = json.dumps(params)
            else:
                params = ''
        if params is None:
            params = ''
        timestamp = str(int(time.time() * 10 ** 3))
        signature = self._sign_request(timestamp, params)
        headers = {
            'X-BAPI-API-KEY': self._api_key,
            'X-BAPI-SIGN': signature,
            'X-BAPI-SIGN-TYPE': '2',
            'X-BAPI-TIMESTAMP': timestamp,
            'X-BAPI-RECV-WINDOW': self._recv_window,
            'Content-Type': 'application/json'
        }
        payload = str(params)
        timeout = self.timeout
        if method == 'POST':
            response = self._session.request(
                method, self._base_url + path, headers=headers,
                data=payload, timeout=timeout
            )
        else:
            response = self._session.request(
                method, self._base_url + path + "?" + payload,
                headers=headers, timeout=timeout
            )
        return self._process_response(response)

    @staticmethod
    def _process_response_batch(data: Dict) -> Any:
        ret_code = data['retCode' if 'retCode' in data else 'ret_code']
        ret_msg = data['retMsg' if 'retMsg' in data else 'ret_msg']
        if ret_code != 0:
            e = ExceptionMapper.from_code(ret_code)
            if e is None:
                raise Exception(ret_msg)
            else:
                raise e(ret_msg)
        return data

    @staticmethod
    def _process_response(response: Response) -> Any:
        try:
            data = response.json()
        except ValueError:
            response.raise_for_status()
            raise
        else:
            ret_code = data['retCode' if 'retCode' in data else 'ret_code']
            ret_msg = data['retMsg' if 'retMsg' in data else 'ret_msg']
            if ret_code != 0:
                e = ExceptionMapper.from_code(ret_code)
                if e is None:
                    raise Exception(ret_msg)
                else:
                    raise e(ret_msg)
            return data['result']

    def get_positions(self, settle_coin: Optional[str] = None, **kwargs) -> List[dict]:
        if self.category == 'spot':
            raise ValueError('Spot does not have positions. Query the balance instead.')
        if settle_coin is None and self.category in {'linear', 'inverse'}:
            if self.category == 'linear':
                settle_coins = {'USDT', 'USDC'}
            else:  # inverse.
                settle_coins = set([a['settleCoin'] for a in self.symbols])
            positions = []
            for settle_coin in settle_coins:
                positions.extend(self.get_positions(settle_coin=settle_coin))
            return positions
        else:
            params = {'category': self.category, 'settleCoin': settle_coin}
            params.update(kwargs)
            path = '/v5/position/list'
            return self._paginate(
                self._get, throttler=self.throttler_positions_list,
                unique_key='symbol', path=path, params=params
            )

    def get_kline(
            self,
            symbol: str,
            interval: str = '1',
            **kwargs
    ):
        params = {'category': self.category, 'symbol': symbol, 'interval': interval}
        params.update(kwargs)
        path = '/v5/market/kline'
        data = self._get(throttler=self.throttler_global, path=path, params=params, post_processing=False)
        columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Turnover']
        data = pd.DataFrame(data['list'], columns=columns).astype(float)
        data['Time'] = data['Time'].apply(lambda x: datetime.utcfromtimestamp(x / 1e3))
        data.set_index('Time', inplace=True)
        data.sort_index(inplace=True)
        return data

    def get_orders(
            self,
            symbol: Optional[str] = None,
            order_id: Optional[str] = None,
            client_id: Optional[str] = None,
            max_records: int = 500,
            **kwargs
    ) -> List[dict]:
        params = {'category': self.category, 'symbol': symbol, 'orderId': order_id, 'orderLinkId': client_id}
        params.update(kwargs)
        path = '/v5/order/history'
        return self._paginate(
            call=self._get, throttler=self.throttler_order_history,
            unique_key='orderId', path=path, params=params, max_records=max_records
        )

    def get_open_orders(self, symbol: Optional[str] = None, settle_coin: Optional[str] = None, **kwargs) -> List[dict]:
        params = {'category': self.category, 'symbol': symbol}
        if symbol is None and settle_coin is None:
            if self.category in {'linear', 'inverse'}:
                open_orders = []
                if self.category == 'linear':
                    settle_coins = {'USDT', 'USDC'}
                else:  # inverse.
                    settle_coins = set([a['settleCoin'] for a in self.symbols])
                for settle_coin in settle_coins:
                    open_orders.extend(self.get_open_orders(settle_coin=settle_coin))
                return open_orders
        else:
            params.update({'settleCoin': settle_coin})
        params.update(kwargs)
        path = '/v5/order/realtime'
        return self._paginate(
            call=self._get, throttler=self.throttler_order_realtime, unique_key='orderId', path=path, params=params
        )

    def get_borrow_date(self, **kwargs):
        path = '/unified/v3/private/account/borrow-rate'
        return self._get(path=path, throttler=self.throttler_global, params=kwargs)

    def get_balances(self, **kwargs) -> dict:
        params = dict(kwargs)
        path = '/v5/account/wallet-balance'
        return {
            at: self._get(
                path, throttler=self.throttler_wallet_balance,
                params={**params, **{'accountType': at}}) for at in
            [ACCOUNT_TYPE_UNIFIED, ACCOUNT_TYPE_CONTRACT]
        }

    def _round(self, symbol: str, price: Optional[float] = None, size: Optional[float] = None) -> float:
        assert price is None or size is None
        if price is not None:
            return round(_round_tick(price, self.tick_sizes[symbol]), 8)
        if size is not None:
            size_ = _round_tick(size, self.step_sizes[symbol])
            if size_ < self.min_quantities[symbol]:
                size_ = self.min_quantities[symbol]
            return round(size_, 8)

    def cancel_order(
            self,
            order_id: Optional[str] = None,
            client_id: Optional[str] = None,
            symbol: Optional[str] = None,
            **kwargs
    ) -> dict:
        if client_id is None:
            assert order_id is not None
        symbol = self._resolve_symbol_from_cache(client_id, order_id, symbol)
        params = {'category': self.category, 'symbol': symbol, 'orderId': order_id, 'orderLinkId': client_id}
        params.update(kwargs)
        path = '/v5/order/cancel'
        return self._post(path=path, throttler=self.throttler_order_cancel, params=params)

    def get_markets(self, symbol: Optional[str] = None, **kwargs) -> List[dict]:
        params = {'category': self.category, 'symbol': symbol}
        params.update(kwargs)
        path = '/v5/market/tickers'
        return self._get(path=path, throttler=self.throttler_global, params=params)

    def get_orderbook(self, symbol: str, depth: Optional[int] = None, **kwargs) -> dict:
        if depth is not None:
            if depth > 500:
                depth = 500
            elif depth < 1:
                depth = 1
        params = {'category': self.category, 'symbol': symbol, 'limit': depth}
        params.update(kwargs)
        path = '/v5/market/orderbook'
        ob = self._get(path=path, throttler=self.throttler_global, params=params)
        ob['bids'] = list(reversed([[float(t[0]), float(t[1])] for t in ob['b']]))
        ob['asks'] = [[float(t[0]), float(t[1])] for t in ob['a']]
        del ob['a']
        del ob['b']
        del ob['u']
        return ob

    def cancel_all_orders(
            self,
            symbol: Optional[str] = None,
            settle_coin: Optional[str] = None,
            **kwargs
    ) -> List[dict]:
        params = {'category': self.category, 'symbol': symbol}
        if symbol is None and settle_coin is None:
            if self.category in {'linear', 'inverse'}:
                deleted_orders = []
                if self.category == 'linear':
                    settle_coins = {'USDT', 'USDC'}
                else:  # inverse.
                    settle_coins = set([a['settleCoin'] for a in self.symbols])
                for settle_coin in settle_coins:
                    deleted_orders.extend(self.cancel_all_orders(settle_coin=settle_coin))
                return deleted_orders
        else:
            params.update({'settleCoin': settle_coin})
            # https://bybit-exchange.github.io/docs/derivativesV3/unified_margin/?console#t-dv_cancelallorders
            # Cancel all coins with quote = USDT.
        params.update(kwargs)
        try:
            path = '/v5/order/cancel-all'
            return self._post(path=path, throttler=self.throttler_order_all_cancel, params=params)
        except Exception as e:
            if str(e).lower() == 'cancel all no result':
                return []
            raise e

    def batch_modify_order(self, params: Dict):
        self.throttler_batch_order_amend.submit()
        return _run_with_timeout(
            func=self.http_session.amend_batch_order,
            timeout_seconds=self.timeout, **params
        )

    def batch_place_order(self, params: Dict):
        self.throttler_batch_order_create.submit()
        return _run_with_timeout(
            func=self.http_session.place_batch_order,
            timeout_seconds=self.timeout, **params
        )

    def modify_order(
            self,
            symbol: Optional[str] = None,
            order_id: Optional[str] = None,
            client_id: Optional[str] = None,
            size: Optional[float] = None,
            price: Optional[float] = None,
            batch: bool = False,
            **kwargs
    ) -> dict:
        # https://bybit-exchange.github.io/docs/v5/order/amend-order
        if client_id is None:
            assert order_id is not None
        symbol = self._resolve_symbol_from_cache(client_id, order_id, symbol)
        price = self._round(symbol, price=price)
        size = self._round(symbol, size=size)
        params = {
            'category': self.category,
            'symbol': symbol,
            'qty': size,
            'price': price,
            'orderId': order_id,
            'orderLinkId': client_id
        }
        params.update(kwargs)
        if client_id is None:
            assert order_id is not None
        if batch:
            batch_key = self.batch_amend.send_request(params)
            data = self.batch_amend.check_result(batch_key, timeout=self.timeout)
            return self._process_response_batch(data)
        else:
            path = '/v5/order/amend'
            return self._post(path=path, params=params, throttler=self.throttler_order_amend)

    def _resolve_symbol_from_cache(
            self,
            client_id: Optional[str] = None,
            order_id: Optional[str] = None,
            symbol: Optional[str] = None
    ):
        if symbol is None:
            try:
                if order_id is not None:
                    symbol = self._cache_order_id_to_symbols[order_id]
                if client_id is not None:
                    symbol = self._cache_client_id_to_symbols[client_id]
            except Exception:
                raise ValueError('Unknown order. Please specify the symbol.')
        return symbol

    # noinspection PyShadowingBuiltins
    def place_order(
            self,
            symbol: str,
            side: str,
            price: Optional[float],
            size: float,
            type: str = 'limit',
            reduce_only: bool = False,
            ioc: bool = False,
            post_only: bool = False,
            client_id: Optional[str] = None,
            use_throttle: bool = True,
            batch: bool = False,
            **kwargs
    ) -> dict:
        type_ = type.lower()
        assert type_ in {'market', 'limit'}
        if type_ == 'market':
            price = None
        elif type_ == 'limit' and price is None:
            raise ValueError('Price should be defined for type=Limit. Maybe you meant type=Market?')
        assert side in {'buy', 'sell'}
        price = self._round(symbol, price=price)
        size = self._round(symbol, size=size)
        tif = None
        if ioc:
            tif = TimeInForce.IOC
        elif post_only:
            tif = TimeInForce.POS
            # https://bybit-exchange.github.io/docs/derivativesV3/unified_margin/#t-dv_placeorder
        params = {
            'symbol': symbol,
            'orderType': type_.title(),
            'side': side.title(),
            'price': price,
            'category': self.category,
            'qty': size,
            'orderLinkId': client_id,
            'reduceOnly': reduce_only,
            'timeInForce': tif,
        }
        params.update(kwargs)
        if batch:
            batch_key = self.batch_create.send_request(params)
            data = self.batch_create.check_result(batch_key, timeout=self.timeout)
            resp = self._process_response_batch(data)
        else:
            path = '/v5/order/create'
            throttle = self.throttler_order_create if use_throttle else None
            resp = self._post(path=path, params=params, throttler=throttle)
        order_id = resp.get('orderId')
        client_id = resp.get('orderLinkId')
        if order_id is not None:
            self._cache_order_id_to_symbols[order_id] = symbol
        if client_id is not None:
            self._cache_client_id_to_symbols[client_id] = symbol
        return resp

    def get_order_history(
            self,
            symbol: Optional[str] = None,
            **kwargs
    ) -> List[dict]:
        path = '/v5/order/history'
        params = {'category': self.category, 'symbol': symbol}
        params.update(kwargs)
        return self._paginate(
            call=self._get, throttler=self.throttler_order_history, unique_key='orderId', path=path, params=params
        )

    def get_risk_limit(self, symbol: Optional[str] = None, **kwargs):
        path = '/v5/market/risk-limit'
        params = {'category': self.category}
        if symbol is not None:
            params['symbol'] = symbol
        params.update(kwargs)
        return self._get(path=path, throttler=self.throttler_global, params=params)

    def set_leverage(self, symbol: str, leverage: int, **kwargs):
        path = '/v5/position/set-leverage'
        params = {
            'category': self.category,
            'symbol': symbol,
            'buyLeverage': str(leverage),
            'sellLeverage': str(leverage)
        }
        params.update(kwargs)
        return self._post(path=path, throttler=self.throttler_global, params=params)

    def set_risk_limit(self, symbol: str, risk_id: int, **kwargs):
        path = '/v5/position/set-risk-limit'
        params = {'category': self.category, 'symbol': symbol, 'riskId': risk_id}
        params.update(kwargs)
        return self._post(path=path, throttler=self.throttler_global, params=params)

    def get_closed_pnl(self, symbol: Optional[str] = None, **kwargs):
        path = '/v5/position/closed-pnl'
        params = {'category': self.category}
        if symbol is not None:
            params['symbol'] = symbol
        params.update(kwargs)
        return self._paginate(
            self._get, path=path, throttler=self.throttler_global, params=params, unique_key='orderId'
        )

    @staticmethod
    def _paginate(call: Callable, unique_key: str, path: str, throttler: ThrottlerFast,
                  params: Dict, max_records: int = int(1e9)) -> List[Dict]:
        records = []
        keys = set()
        past_cursors = set()
        cursor = None
        first_step = True
        params['limit'] = '100'  # pagination limit.
        while first_step or cursor is not None:
            first_step = False
            params['cursor'] = cursor
            if cursor not in past_cursors:
                past_cursors.add(cursor)
            else:
                break
            results = call(path=path, params=params, throttler=throttler, pagination=True)
            if len(results) == 0:
                break
            old_key_count = len(keys)
            for result in results['list']:
                if result[unique_key] not in keys:
                    keys.add(result[unique_key])
                    records.append(result)
            new_key_count = len(keys)
            if new_key_count - old_key_count < 20:
                break
            cursor = results.get('nextPageCursor')
            if len(records) >= max_records:
                records = records[:max_records]
                break
        return records

    def get_instruments_info(self, **kwargs) -> List[dict]:
        params = {'category': self.category}
        params.update(kwargs)
        path = '/v5/market/instruments-info'
        return self._paginate(
            call=self._get,
            throttler=self.throttler_global,
            unique_key='symbol',
            path=path,
            params=params
        )

    def fetch_perp_markets(self, **kwargs) -> List[Dict]:
        return [a for a in self.get_instruments_info(**kwargs) if a['quoteCoin'] == 'USDT']

    def get_trade_history(self, **kwargs) -> List[dict]:
        path = '/v5/account/transaction-log'
        params = {'category': self.category}
        params.update(kwargs)
        return self._paginate(
            call=self._get, unique_key='tradeId', path=path, throttler=self.throttler_transaction_log, params=params
        )

    def get_executions(
            self,
            symbol: Optional[str] = None,
            order_id: Optional[str] = None,
            client_id: Optional[str] = None,
            max_records: int = 500,
            **kwargs
    ):
        params = {'category': self.category, 'symbol': symbol, 'orderId': order_id, 'orderLinkId': client_id}
        params.update(kwargs)
        path = '/v5/execution/list'
        return self._paginate(
            call=self._get, unique_key='execId', path=path, params=params,
            max_records=max_records, throttler=self.throttler_executions_list
        )

    def get_funding_history(self, symbol: str, **kwargs):
        params = {'category': self.category, 'symbol': symbol}
        params.update(kwargs)
        path = '/v5/market/funding/history'
        return self._get(path=path, throttler=self.throttler_global, params=params)


class ByBitStream:
    TOPIC_ORDER = 'TOPIC_ORDER'
    TOPIC_POSITION = 'TOPIC_POSITION'
    TOPIC_EXECUTION = 'TOPIC_EXECUTION'

    def __init__(
            self,
            credentials: Union[Path, str, None] = None,
            subscribe_to_order_books: bool = True,
            subscribe_to_tickers: bool = True,
            orderbook_depth: int = 50,
            private: bool = False,
            category: str = 'linear',
            background: bool = False,
            print_stats_every: int = 600,
    ):
        self.background = background
        self.private_topics = {
            self.TOPIC_ORDER: 'order',
            self.TOPIC_POSITION: 'position',
            self.TOPIC_EXECUTION: 'execution',
        }
        self.private = private
        if credentials is not None:
            credentials = _read_credentials(Path(credentials).expanduser())
            self.api_key = credentials['apiKey']
            self.api_secret = credentials['secret']
        else:
            self.api_key = None
            self.api_secret = None
        if self.private:
            self.url = 'wss://stream.bybit.com/v5/private'
        else:
            self.url = f'wss://stream.bybit.com/v5/public/{category}'
        logger.info(f'URL: {self.url}.')
        our_time = time.time()
        bybit_time = float(requests.get('https://api.bybit.com/v5/market/time').json()['result']['timeNano']) / 1e9
        self.latency_offset = bybit_time - our_time
        logger.info(f'Latency offset is {self.latency_offset} seconds (adjusted).')
        if self.latency_offset < 0:
            self.latency_offset = 0
        self._last_debug_ts = None
        self.print_status_every = print_stats_every
        self.latency_per_sub = defaultdict(list)
        # https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
        assert orderbook_depth in [1, 50, 200, 500]
        self._subscribe_to_order_books = subscribe_to_order_books
        self._subscribe_to_tickers = subscribe_to_tickers
        self.orderbook_depth = orderbook_depth
        self.orderbook_handler = ByBitOrderBooks()
        self.ticker_handler = ByBitRealTimeTickers()
        self.order_status_handler = ByBitRealTimeOrderStatuses()
        self.position_handler = ByBitRealTimePositions()
        self.execution_handler = ByBitExecutions()
        self.order_book_symbols = set()
        self.tickers_symbols = set()
        self.delay = 0.0
        self.counter = 0
        self.ws = None
        self._ready = False
        self._conn_ws()

    def wait_until_ready(self):
        while not self._ready:
            time.sleep(0.1)

    def send_auth(self, ws):
        key = self.api_key
        secret = self.api_secret
        expires = int((time.time() + 10) * 1000)
        _val = f'GET/realtime{expires}'
        signature = str(hmac.new(
            bytes(secret, 'utf-8'),
            bytes(_val, 'utf-8'), digestmod='sha256'
        ).hexdigest())
        ws.send(json.dumps({'op': 'auth', 'args': [key, expires, signature]}))

    @property
    def type(self):
        return 'prv' if self.private else 'pub'

    # noinspection PyUnusedLocal
    def _on_message(self, ws, message):
        data = orjson.loads(message)
        try:
            # https://api.bybit.com/v2/public/time
            # https://bybit-exchange.github.io/docs/futuresV2/inverse/#t-api
            if 'op' in data and data['op'] == 'subscribe':
                if data['success']:
                    logger.info(f'Subscription successful.')
                    return
                else:
                    raise Exception('Could not subscribe.')
            if 'type' in data and data['type'] == 'AUTH_RESP':
                if data['success']:
                    logger.info('Successfully authenticated.')
                    return
                else:
                    raise Exception('Could not authenticate.')
            if 'type' in data and data['type'] == 'COMMAND_RESP':
                logger.info('Command response received.')
                return
            if 'op' in data and data['op'] == 'auth':
                logger.info(f'Authenticated. Connection ID is {data["conn_id"]}.')
                return
            if 'topic' in data:
                # noinspection PyBroadException
                try:
                    self.delay += max((time.time() - _convert_ts(data['ts'])) * 1e3, 0)
                    self.counter += 1
                except Exception:
                    pass
                topic = data['topic']
                # self._print_stats(data, topic)
                if topic.startswith('orderbook'):
                    self.orderbook_handler.on_message(data)
                elif topic.startswith('tickers'):
                    self.ticker_handler.on_message(data)
                elif topic == self.private_topics[self.TOPIC_ORDER]:
                    self.order_status_handler.on_message(data)
                elif topic == self.private_topics[self.TOPIC_POSITION]:
                    self.position_handler.on_message(data)
                elif topic == self.private_topics[self.TOPIC_EXECUTION]:
                    self.execution_handler.on_message(data)
                else:
                    logger.warning(f'Unknown topic {data}.')
            else:
                logger.info(f'FILTER: {data}')
        except Exception as e:
            logger.exception(str(e))
            logger.warning(f'ERROR: {data} {str(e)}.')

    # def _print_stats(self, data, topic):
    #     if self.print_status_every <= 0:
    #         return
    #     if self.private:
    #         return
    #     ts = data['ts'] / 1e3
    #     our_ts = time.time()
    #     latency = self.latency_offset + our_ts - ts
    #     self.latency_per_sub[topic].append(latency)
    #     if self._last_debug_ts is None:
    #         self._last_debug_ts = ts
    #     if ts - self._last_debug_ts > self.print_status_every:
    #         self._last_debug_ts = ts
    #         if self.subscribe_to_tickers:
    #             ticker_latencies = sum([b for a, b in self.latency_per_sub.items() if a.startswith('tickers')], [])
    #             ticker_mean = np.mean(ticker_latencies) * 1000
    #             ticker_median = np.median(ticker_latencies) * 1000
    #             logger.info(f'Stats: tickers: mean/median/count {ticker_mean:.2f}ms/'
    #                         f'{ticker_median:.2f}ms/{len(ticker_latencies)}, '
    #                         f'(interval: {self.print_status_every}s).')
    #         if self.subscribe_to_order_books:
    #             orderbook_latencies = sum([b for a, b in self.latency_per_sub.items() if a.startswith('orderbook')], [])
    #             orderbook_mean = np.mean(orderbook_latencies) * 1000
    #             orderbook_median = np.median(orderbook_latencies) * 1000
    #             logger.info(f'Stats: orderbook: mean/median/count {orderbook_mean:.2f}ms/'
    #                         f'{orderbook_median:.2f}ms/{len(orderbook_latencies)},'
    #                         f' (interval: {self.print_status_every}s).')
    #         self.latency_per_sub.clear()

    # noinspection PyUnusedLocal
    @staticmethod
    def _on_error(ws, error, *args, **kwargs):
        logger.warning(f'Error: {error}.')

    # noinspection PyUnusedLocal
    @staticmethod
    def _on_close(ws, *args, **kwargs):
        logger.warning('Feed closed.')
        time.sleep(5)
        ws.run_forever(ping_interval=20, ping_timeout=10)

    # noinspection PyUnusedLocal
    def _on_open(self, ws, *args, **kwargs):
        logger.debug('Open feed.')
        topics = []
        if self.private:
            logger.debug('Auth.')
            self.send_auth(ws)
            topics = list(self.private_topics.values())
        self.subscribe(topics)
        self._ready = True

    def subscribe_to_tickers(self, symbols: List[str], batch_size: int = 20, wait: float = 0.5):
        topics = []
        for symbol in symbols:
            if symbol not in self.tickers_symbols:
                self.tickers_symbols.add(symbol)
                topics.append(symbol)
        batches = [topics[i:i + batch_size] for i in range(0, len(topics), batch_size)]
        for batch in batches:
            self.subscribe([f'tickers.{symbol}' for symbol in batch])
            time.sleep(wait)

    def subscribe_to_ticker(self, symbol: str):
        if symbol not in self.tickers_symbols:
            self.tickers_symbols.add(symbol)
            self.subscribe([f'tickers.{symbol}'])

    def subscribe_to_orderbook(self, symbol: str, depth: Optional[int] = None):
        depth = depth if depth is not None else self.orderbook_depth
        if symbol not in self.order_book_symbols:
            self.order_book_symbols.add(symbol)
            self.subscribe([f'orderbook.{depth}.{symbol}'])

    def subscribe(self, topics: List[str]):
        if len(topics) == 0:
            return
        topics_str = ",".join(topics)
        if len(topics) > 100:
            topics_str = topics_str[0:40] + ' [ -> ] ' + topics_str[-40:] + ' (ALL)'
        logger.info(f'Subscribing to {topics_str}.')
        self.ws.send(json.dumps({'op': 'subscribe', 'args': topics}))

    # noinspection PyUnusedLocal
    @staticmethod
    def _on_pong(*data):
        logger.debug('Pong received.')

    # noinspection PyUnusedLocal
    @staticmethod
    def _on_ping(ws, *data):
        logger.debug(f'Ping sent {datetime.now()}.')

    def _run_forever(self):
        self.ws.run_forever(
            ping_interval=20,
            ping_timeout=10
        )

    def _conn_ws(self):
        self.ws = websocket.WebSocketApp(
            url=self.url,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_ping=self._on_ping,
            on_pong=self._on_pong,
            on_open=self._on_open
        )
        if self.background:
            _runs_in_a_thread(self._run_forever, name=f'WS_{self.type}')
            self.wait_until_ready()
        else:
            self._run_forever()

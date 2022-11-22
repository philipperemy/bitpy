import hashlib
import hmac
import json
import logging
import threading
import time
from collections import defaultdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from queue import Queue, Empty
from typing import Optional, Dict, Any, List, Callable
from typing import Union
from urllib.parse import urlencode

import numpy as np
import requests
import websocket
from requests import Session, Response
from urllib3 import HTTPSConnectionPool

logger = logging.getLogger(__name__)


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
    }

    @staticmethod
    def from_code(code: int):
        return ExceptionMapper.exception_map.get(str(code))


# https://bybit-exchange.github.io/docs/derivativesV3/unified_margin

def _result_to_float_values(d: Union[List, dict]) -> Union[dict, List[float]]:
    res = {}
    if isinstance(d, list):
        return [float(a) for a in d]
    for k, v in d.items():
        if isinstance(v, list):
            res[k] = [_result_to_float_values(a) for a in v]
        elif isinstance(v, dict):
            res[k] = _result_to_float_values(v)
        else:
            try:
                if v is None:
                    res[k] = v
                else:
                    res[k] = float(v)
            except ValueError:
                res[k] = v
    return res


class ByBitThrottler:

    def __init__(self, waiting_time=0.02):  # 50 requests / second.
        self.requests = Queue()
        self.responses = Queue()
        self.waiting_time = waiting_time
        _runs_in_a_thread(self._run, name='Throttle')

    def _run(self):
        while True:
            try:
                self.requests.get_nowait()
                time.sleep(self.waiting_time)
            except Empty:
                time.sleep(0.001)

    def submit_and_wait(self):
        req_time = time.time()
        self.requests.put('req')
        self.responses.get()
        resp_time = time.time()
        delay = resp_time - req_time
        if delay > 0.2:
            logger.warning(f'Throttling delay detected: {int(delay * 1000)} ms.')


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


# fixed list.
PERP_LIST = [
    '10000NFTUSDT', '1000BTTUSDT', '1000LUNCUSDT', '1000XECUSDT', '1INCHUSDT', 'AAVEUSDT', 'ACHUSDT',
    'ADAUSDT', 'AGLDUSDT', 'AKROUSDT', 'ALGOUSDT', 'ALICEUSDT', 'ALPHAUSDT', 'ANKRUSDT', 'ANTUSDT', 'APEUSDT',
    'API3USDT', 'APTUSDT', 'ARPAUSDT', 'ARUSDT', 'ASTRUSDT', 'ATOMUSDT', 'AUDIOUSDT', 'AVAXUSDT', 'AXSUSDT',
    'BAKEUSDT', 'BALUSDT', 'BANDUSDT', 'BATUSDT', 'BCHUSDT', 'BELUSDT', 'BICOUSDT', 'BITUSDT', 'BLZUSDT',
    'BNBUSDT', 'BNXUSDT', 'BOBAUSDT', 'BSVUSDT', 'BSWUSDT', 'BTCUSDT', 'C98USDT', 'CEEKUSDT', 'CELOUSDT',
    'CELRUSDT', 'CHRUSDT', 'CHZUSDT', 'CKBUSDT', 'COMPUSDT', 'COTIUSDT', 'CREAMUSDT', 'CROUSDT', 'CRVUSDT',
    'CTCUSDT', 'CTKUSDT', 'CTSIUSDT', 'CVCUSDT', 'CVXUSDT', 'DARUSDT', 'DASHUSDT', 'DENTUSDT', 'DGBUSDT',
    'DODOUSDT', 'DOGEUSDT', 'DOTUSDT', 'DUSKUSDT', 'DYDXUSDT', 'EGLDUSDT', 'ENJUSDT', 'ENSUSDT', 'EOSUSDT',
    'ETCUSDT', 'ETHUSDT', 'ETHWUSDT', 'FILUSDT', 'FITFIUSDT', 'FLMUSDT', 'FLOWUSDT', 'FTMUSDT', 'FXSUSDT',
    'GALAUSDT', 'GALUSDT', 'GLMRUSDT', 'GMTUSDT', 'GMXUSDT', 'GRTUSDT', 'GTCUSDT', 'HBARUSDT', 'HNTUSDT',
    'HOTUSDT', 'ICPUSDT', 'ICXUSDT', 'ILVUSDT', 'IMXUSDT', 'INJUSDT', 'IOSTUSDT', 'IOTAUSDT', 'IOTXUSDT',
    'JASMYUSDT', 'JSTUSDT', 'KAVAUSDT', 'KDAUSDT', 'KLAYUSDT', 'KNCUSDT', 'KSMUSDT', 'LDOUSDT', 'LINAUSDT',
    'LINKUSDT', 'LITUSDT', 'LOOKSUSDT', 'LPTUSDT', 'LRCUSDT', 'LTCUSDT', 'LUNA2USDT', 'MANAUSDT', 'MASKUSDT',
    'MATICUSDT', 'MINAUSDT', 'MKRUSDT', 'MTLUSDT', 'NEARUSDT', 'NEOUSDT', 'OCEANUSDT', 'OGNUSDT', 'OMGUSDT',
    'ONEUSDT', 'ONTUSDT', 'OPUSDT', 'PAXGUSDT', 'PEOPLEUSDT', 'QTUMUSDT', 'REEFUSDT', 'RENUSDT', 'REQUSDT',
    'RNDRUSDT', 'ROSEUSDT', 'RSRUSDT', 'RSS3USDT', 'RUNEUSDT', 'RVNUSDT', 'SANDUSDT', 'SCRTUSDT', 'SCUSDT',
    'SFPUSDT', 'SHIB1000USDT', 'SKLUSDT', 'SLPUSDT', 'SNXUSDT', 'SOLUSDT', 'SPELLUSDT', 'STGUSDT', 'STMXUSDT',
    'STORJUSDT', 'STXUSDT', 'SUNUSDT', 'SUSHIUSDT', 'SWEATUSDT', 'SXPUSDT', 'THETAUSDT', 'TLMUSDT', 'TOMOUSDT',
    'TRBUSDT', 'TRXUSDT', 'TWTUSDT', 'UNFIUSDT', 'UNIUSDT', 'USDCUSDT', 'VETUSDT', 'WAVESUSDT', 'WOOUSDT',
    'XCNUSDT', 'XEMUSDT', 'XLMUSDT', 'XMRUSDT', 'XNOUSDT', 'XRPUSDT', 'XTZUSDT', 'YFIUSDT', 'YGGUSDT',
    'ZECUSDT', 'ZENUSDT', 'ZILUSDT', 'ZRXUSDT'
]


class ByBit:

    def __init__(
            self,
            credentials: Optional[Union[Path, str]] = None,
            subscribe_to_order_books: bool = False,
            subscribe_to_tickers: bool = False,
            subscribe_to_private_feed: bool = True,
            orderbook_depth: int = 25,
            category: str = 'linear',
            base_url: str = "https://api.bybit.com",
            timeout: int = 3,
    ):
        self.credentials = credentials
        self.rest = ByBitRest.from_credentials_file(
            self.credentials, category=category,
            base_url=base_url, timeout=timeout
        )
        self.subscribe_to_order_books = subscribe_to_order_books
        self.subscribe_to_tickers = subscribe_to_tickers
        self.orderbook_depth = orderbook_depth
        self.public_feed = None
        if self.subscribe_to_order_books or self.subscribe_to_tickers:
            self.public_feed = ByBitStream(
                credentials=None,
                subscribe_to_order_books=self.subscribe_to_order_books,
                subscribe_to_tickers=self.subscribe_to_tickers,
                orderbook_depth=self.orderbook_depth,
                private=False, background=True,
                rest_api=self.rest
            )
        self.private_feed = None
        if subscribe_to_private_feed and self.credentials is not None:
            self.private_feed = ByBitStream(self.credentials, private=True, background=True, rest_api=self.rest)

    def get_positions(self, symbol: Optional[str] = None, **kwargs) -> List[dict]:
        if self.private_feed is not None:
            positions = self.private_feed.position_handler.positions
            if symbol is not None:
                symbol_position = positions.get(symbol)
                return [] if symbol_position is None else [symbol_position]
            else:
                list(positions.values())
        return self.rest.get_positions(symbol, **kwargs)

    def get_orderbook(self, symbol: str, depth: Optional[int] = None) -> dict:
        if self.subscribe_to_order_books:
            return self.public_feed.orderbook_handler.get_orderbook(symbol)
        return self.rest.get_orderbook(symbol, depth)

    def get_orders(
            self,
            symbol: Optional[str] = None,
            order_id: Optional[str] = None,
            client_id: Optional[str] = None,
            **kwargs
    ) -> List[dict]:
        return self.rest.get_orders(symbol, order_id, client_id, **kwargs)

    def get_open_orders(self, symbol: Optional[str] = None, **kwargs) -> List[dict]:
        return self.rest.get_open_orders(symbol, **kwargs)

    def get_tickers(self, symbol: Optional[str] = None, **kwargs):
        if self.subscribe_to_tickers:
            tickers = self.public_feed.ticker_handler.tickers
            for t in tickers.values():
                t['bidPrice'] = t['bid1Price']
                t['askPrice'] = t['ask1Price']
            return tickers.get(symbol) if symbol is not None else tickers
        return self.rest.get_markets(symbol, **kwargs)

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
            **kwargs
    ) -> dict:
        return self.rest.modify_order(symbol, order_id, client_id, size, price, **kwargs)

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
            **kwargs
    ) -> dict:
        return self.rest.place_order(symbol, side, price, size, type, reduce_only, ioc, post_only, client_id, **kwargs)

    def cancel_order(
            self,
            symbol: Optional[str] = None,
            order_id: Optional[str] = None,
            client_id: Optional[str] = None,
            **kwargs
    ):
        return self.rest.cancel_order(symbol, order_id, client_id, **kwargs)

    def cancel_all_orders(
            self,
            symbol: Optional[str] = None,
            only_conditional_orders: bool = False,
            only_limit_orders: bool = False,
            **kwargs
    ) -> dict:
        return self.rest.cancel_all_orders(symbol, only_conditional_orders, only_limit_orders, **kwargs)

    def get_balances(self, **kwargs):
        return self.rest.get_balances(**kwargs)

    def get_executions(
            self,
            symbol: Optional[str] = None,
            order_id: Optional[str] = None,
            client_id: Optional[str] = None,
            **kwargs
    ) -> List[dict]:
        if self.private_feed is not None:
            return self.private_feed.execution_handler.get_executions(symbol, order_id, client_id)
        return self.rest.get_executions(symbol, order_id, client_id, **kwargs)


class TimeInForce(Enum):
    GTC = 'GoodTillCancel'
    IOC = 'ImmediateOrCancel'
    FOK = 'FillOrKill'
    POS = 'PostOnly'


class OrderFilter(Enum):
    ORD = 'Order'
    STOP = 'StopOrder'


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
        for result in data['result']:
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


class ByBitPositions:
    def __init__(self):
        self.positions = {}

    def on_message(self, msg: dict):
        data = msg['data']
        data = _result_to_float_values(data)
        for result in data['result']:
            # {'positionIdx': 0, 'riskId': 1, 'symbol': 'BTCUSDT', 'side': 'None', 'size': '0.0000',
            # 'entryPrice': '0.00000000', 'leverage': '10', 'markPrice': '16851.50000000',
            # 'positionIM': '0.00000000', 'positionMM': '0.00000000', 'takeProfit': '',
            # 'stopLoss': '', 'trailingStop': '', 'positionValue': '0.00000000',
            # 'unrealisedPnl': '0.00000000', 'cumRealisedPnl': '0.00000000',
            # 'createdTime': 1668743829404, 'updatedTime': 1668919315146, '
            # tpslMode': 'Full', 'sessionAvgPrice': ''}
            self.positions[result['symbol']] = result


class ByBitOrderStatuses:
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
        for result in data['result']:
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
            short_order_id = order_id.split('-')[-1]
            logger.info(f'OrderID:{short_order_id} {result["orderStatus"]} '
                        f'{result["side"]} {result["symbol"]} '
                        f'{float(result["qty"])}@{float(result["price"])}, '
                        f'CumExecQty={float(result["cumExecQty"])}, ReduceOnly={1 if result["reduceOnly"] else 0}.')


class ByBitTickers:

    def __init__(self):
        self.tickers = {}

    def on_message(self, msg: dict):
        data = msg['data']
        symbol = data['symbol']
        data = _result_to_float_values(data)
        if symbol not in self.tickers or msg['type'] == 'snapshot':
            self.tickers[symbol] = data
        else:
            self.tickers[symbol].update(data)

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
            category: str = 'linear'
    ) -> None:
        self.timeout = timeout
        self.category = category
        # self.throttler = ByBitThrottler()
        self._session = Session()
        self._base_url = base_url
        if api_key is None:
            api_key = ''
        if api_secret is None:
            api_secret = ''
        self._api_key = api_key
        self._api_secret = api_secret
        self._recv_window = str(5000)
        self._symbols = self.query_symbols_v2()
        self.step_sizes = {s['name']: float(s['lot_size_filter']['qty_step']) for s in self._symbols}
        self.min_quantities = {s['name']: float(s['lot_size_filter']['min_trading_qty']) for s in self._symbols}
        # https://bybit-exchange.github.io/docs/derivativesV3/unified_margin/#t-ipratelimits
        self.tick_prices = {s['name']: float(s['price_filter']['tick_size']) for s in self._symbols}
        # Why? Modify requires the symbol but we can cache it during place_order.
        self._cache_order_id_to_symbols = {}
        self._cache_client_id_to_symbols = {}

    @classmethod
    def from_credentials_file(cls, credentials: Union[Path, str, None] = None, **kwargs):
        api_key = None
        api_secret = None
        if credentials is not None:
            credentials = _read_credentials(Path(credentials).expanduser())
            api_key = credentials['apiKey']
            api_secret = credentials['secret']
        return cls(
            api_key=api_key,
            api_secret=api_secret,
            **kwargs
        )

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
    def _retry_on_error(call, retries: int = 5, wait=0.0, verbose=True, *args, **kwargs):
        for i in range(retries):
            try:
                return call(*args, **kwargs)
            except Exception as e:
                if 'timed out' in str(e) or \
                        'timeout' in str(e) or \
                        isinstance(e, TimeoutError) or \
                        isinstance(e, HTTPSConnectionPool):
                    if verbose:
                        logger.warning(f'{call.__name__}, args: {args}, kwargs: {kwargs}. Error: {str(e)}.')
                    if i == retries - 1:
                        raise e
                    if wait > 0:
                        time.sleep(wait)
                else:
                    raise e

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None, pagination: bool = False) -> Any:
        # self.throttler.submit_and_wait()
        req = self._retry_on_error(self._request, method='GET', path=path, params=params)
        return self._post_processing(req, pagination=pagination)

    def _post(self, path: str, params: Optional[Dict[str, Any]] = None, pagination: bool = False) -> Any:
        # self.throttler.submit_and_wait()
        req = self._retry_on_error(self._request, method='POST', path=path, params=params)
        return self._post_processing(req, pagination=pagination)

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

    def get_positions(self, symbol: Optional[str] = None, **kwargs) -> List[dict]:
        params = {'category': self.category, 'symbol': symbol}
        params.update(kwargs)
        return self._paginate(self._get, unique_key='symbol', path='/unified/v3/private/position/list', params=params)

    def get_orders(
            self,
            symbol: Optional[str] = None,
            order_id: Optional[str] = None,
            client_id: Optional[str] = None,
            **kwargs
    ) -> List[dict]:
        params = {'category': self.category, 'symbol': symbol, 'orderId': order_id, 'orderLinkId': client_id}
        params.update(kwargs)
        path = '/unified/v3/private/order/list'
        return self._paginate(call=self._get, unique_key='orderId', path=path, params=params)

    def get_open_orders(self, symbol: Optional[str] = None, **kwargs) -> List[dict]:
        params = {'category': self.category, 'symbol': symbol}
        params.update(kwargs)
        path = '/unified/v3/private/order/unfilled-orders'
        return self._paginate(call=self._get, unique_key='orderId', path=path, params=params)

    def get_balances(self, **kwargs) -> List[dict]:
        params = dict(kwargs)
        return self._get('/unified/v3/private/account/wallet/balance', params=params)

    def query_symbols_v2(self, **kwargs) -> List[dict]:
        params = dict(kwargs)
        return self._get('/v2/public/symbols', params=params)

    def _round(self, symbol: str, price: Optional[float] = None, size: Optional[float] = None) -> float:
        assert price is None or size is None
        if price is not None:
            return round(_round_tick(price, self.tick_prices[symbol]), 8)
        if size is not None:
            size_ = _round_tick(size, self.step_sizes[symbol])
            if size_ < self.min_quantities[symbol]:
                size_ = self.min_quantities[symbol]
            return round(size_, 8)

    def cancel_order(
            self,
            symbol: Optional[str] = None,
            order_id: Optional[str] = None,
            client_id: Optional[str] = None,
            **kwargs
    ) -> dict:
        if client_id is None:
            assert order_id is not None
        symbol = self._resolve_symbol_from_cache(client_id, order_id, symbol)
        params = {'category': self.category, 'symbol': symbol, 'orderId': order_id, 'orderLinkId': client_id}
        params.update(kwargs)
        return self._post('/unified/v3/private/order/cancel', params)

    def get_markets(self, symbol: Optional[str] = None, **kwargs) -> List[dict]:
        params = {'category': self.category, 'symbol': symbol}
        params.update(kwargs)
        return self._get('/derivatives/v3/public/tickers', params)

    def get_orderbook(self, symbol: str, depth: Optional[int] = None, **kwargs) -> dict:
        if depth is not None:
            if depth > 500:
                depth = 500
            elif depth < 1:
                depth = 1
        params = {'category': self.category, 'symbol': symbol, 'limit': depth}
        params.update(kwargs)
        ob = self._get('/derivatives/v3/public/order-book/L2', params)
        ob['bids'] = list(reversed([[float(t[0]), float(t[1])] for t in ob['b']]))
        ob['asks'] = [[float(t[0]), float(t[1])] for t in ob['a']]
        del ob['a']
        del ob['b']
        del ob['u']
        return ob

    def cancel_all_orders(
            self,
            symbol: Optional[str] = None,
            only_conditional_orders: bool = False,
            only_limit_orders: bool = False,
            **kwargs
    ) -> Optional[dict]:
        order_filter = None
        if only_conditional_orders:
            order_filter = OrderFilter.STOP
        elif only_limit_orders:
            order_filter = OrderFilter.ORD
        # if order_filter is not None and symbol is None:
        #     raise ValueError('Symbol should be specified if only_limit_orders=True or only_conditional_orders=True')
        params = {'category': self.category, 'symbol': symbol, 'orderFilter': order_filter}
        if symbol is None:
            # https://bybit-exchange.github.io/docs/derivativesV3/unified_margin/?console#t-dv_cancelallorders
            # Cancel all coins with quote = USDT.
            params['settleCoin'] = 'USDT'
        params.update(kwargs)
        try:
            return self._post('/unified/v3/private/order/cancel-all', params)
        except Exception as e:
            if str(e).lower() == 'cancel all no result':
                return None
            raise e

    def modify_order(
            self,
            symbol: Optional[str] = None,
            order_id: Optional[str] = None,
            client_id: Optional[str] = None,
            size: Optional[float] = None,
            price: Optional[float] = None,
            **kwargs
    ) -> dict:
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
        return self._post(
            path='/unified/v3/private/order/replace',
            params=params
        )

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
                raise ValueError('Unknown order to modify. Please specify the symbol.')
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
        resp = self._post(path='/unified/v3/private/order/create', params=params)
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
        path = '/unified/v3/private/order/list'
        params = {
            'category': self.category,
            'symbol': symbol,
        }
        params.update(kwargs)
        return self._paginate(call=self._get, unique_key='orderId', path=path, params=params)

    @staticmethod
    def _paginate(call: Callable, unique_key: str, path: str, params: Dict) -> List[Dict]:
        records = []
        keys = set()
        past_cursors = set()
        cursor = None
        first_step = True
        params['limit'] = '1000'  # pagination limit.
        while first_step or cursor is not None:
            first_step = False
            params['cursor'] = cursor
            if cursor not in past_cursors:
                past_cursors.add(cursor)
            else:
                break
            results = call(path=path, params=params, pagination=True)
            if len(results) == 0:
                break
            cursor = results['nextPageCursor']
            old_key_count = len(keys)
            for result in results['list']:
                if result[unique_key] not in keys:
                    keys.add(result[unique_key])
                    records.append(result)
            new_key_count = len(keys)
            if new_key_count - old_key_count < 20:
                break
        return records

    def _get_instruments_info(self, **kwargs) -> List[dict]:
        params = {'category': self.category}
        params.update(kwargs)
        return self._paginate(
            self._get,
            unique_key='symbol',
            path='/derivatives/v3/public/instruments-info',
            params=params
        )

    def fetch_perp_markets(self, **kwargs) -> List[Dict]:
        return [a for a in self._get_instruments_info(**kwargs) if a['quoteCoin'] == 'USDT']

    def get_executions(
            self,
            symbol: Optional[str] = None,
            order_id: Optional[str] = None,
            client_id: Optional[str] = None,
            **kwargs):
        params = {'category': self.category, 'symbol': symbol, 'orderId': order_id, 'orderLinkId': client_id}
        params.update(kwargs)
        return self._paginate(
            self._get,
            unique_key='execId',
            path='/unified/v3/private/execution/list',
            params=params
        )


class ByBitStream:

    def __init__(
            self,
            credentials: Union[Path, str, None] = None,
            subscribe_to_order_books: bool = True,
            subscribe_to_tickers: bool = True,
            orderbook_depth: int = 25,
            private: bool = False,
            background: bool = False,
            print_stats_every: int = 600,
            rest_api: Optional[ByBitRest] = None
    ):
        self.rest_api = rest_api
        self.background = background
        self.private_topics = [
            'user.order.unifiedAccount',
            'user.position.unifiedAccount',
            'user.execution.unifiedAccount',
        ]
        self.private = private
        if credentials is not None:
            credentials = _read_credentials(Path(credentials).expanduser())
            self.api_key = credentials['apiKey']
            self.api_secret = credentials['secret']
        else:
            self.api_key = None
            self.api_secret = None
        if self.private:
            self.url = 'wss://stream.bybit.com/unified/private/v3'
        else:
            self.url = 'wss://stream.bybit.com/contract/usdt/public/v3'
        logger.info(f'WEBSOCKET: url: {self.url}.')
        bybit_time = float(requests.get('https://api.bybit.com/v2/public/time').json()['time_now'])
        our_time = time.time()
        self.latency_offset = bybit_time - our_time
        if self.latency_offset < 0:
            self.latency_offset = 0
        self._last_debug_ts = None
        self.print_status_every = print_stats_every
        self.latency_per_sub = defaultdict(list)
        assert orderbook_depth in [1, 25, 50, 100, 200, 500]
        self.subscribe_to_order_books = subscribe_to_order_books
        self.subscribe_to_tickers = subscribe_to_tickers
        self.orderbook_depth = orderbook_depth
        self.orderbook_handler = ByBitOrderBooks()
        self.ticker_handler = ByBitTickers()
        self.order_status_handler = ByBitOrderStatuses()
        self.position_handler = ByBitPositions()
        self.execution_handler = ByBitExecutions()
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
        # print(_val)
        signature = str(hmac.new(
            bytes(secret, 'utf-8'),
            bytes(_val, 'utf-8'), digestmod='sha256'
        ).hexdigest())
        ws.send(json.dumps({'op': 'auth', 'args': [key, expires, signature]}))

    # noinspection PyUnusedLocal
    def _on_message(self, ws, message):
        logger.debug(message)
        data = json.loads(message)
        try:
            # https://api.bybit.com/v2/public/time
            # https://bybit-exchange.github.io/docs/futuresV2/inverse/#t-api
            if 'op' in data and data['op'] == 'subscribe':
                if data['success']:
                    logger.info('Successfully subscribed.')
                    self._ready = True
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
                self._ready = True
                return

            if 'topic' in data:
                topic = data['topic']
                self._print_stats(data, topic)
                if topic.startswith('orderbook'):
                    self.orderbook_handler.on_message(data)
                elif topic.startswith('tickers'):
                    self.ticker_handler.on_message(data)
                elif topic == 'user.order.unifiedAccount':
                    self.order_status_handler.on_message(data)
                elif topic == 'user.position.unifiedAccount':
                    self.position_handler.on_message(data)
                elif topic == 'user.execution.unifiedAccount':
                    self.execution_handler.on_message(data)
            else:
                logger.info(f'FILTER: {data}')
        except Exception as e:
            logger.exception(str(e))
            logger.warning(f'ERROR: {data} {str(e)}.')

    def _print_stats(self, data, topic):
        if self.print_status_every <= 0:
            return
        if self.private:
            return
        ts = data['ts'] / 1e3
        our_ts = time.time()
        latency = self.latency_offset + our_ts - ts
        self.latency_per_sub[topic].append(latency)
        if self._last_debug_ts is None:
            self._last_debug_ts = ts
        if ts - self._last_debug_ts > self.print_status_every:
            self._last_debug_ts = ts
            if self.subscribe_to_tickers:
                ticker_latencies = sum([b for a, b in self.latency_per_sub.items() if a.startswith('tickers')], [])
                ticker_mean = np.mean(ticker_latencies) * 1000
                ticker_median = np.median(ticker_latencies) * 1000
                logger.info(f'Stats: tickers: mean/median/count {ticker_mean:.2f}ms/'
                            f'{ticker_median:.2f}ms/{len(ticker_latencies)}, '
                            f'(interval: {self.print_status_every}s).')
            if self.subscribe_to_order_books:
                orderbook_latencies = sum([b for a, b in self.latency_per_sub.items() if a.startswith('orderbook')], [])
                orderbook_mean = np.mean(orderbook_latencies) * 1000
                orderbook_median = np.median(orderbook_latencies) * 1000
                logger.info(f'Stats: orderbook: mean/median/count {orderbook_mean:.2f}ms/'
                            f'{orderbook_median:.2f}ms/{len(orderbook_latencies)},'
                            f' (interval: {self.print_status_every}s).')
            self.latency_per_sub.clear()

    # noinspection PyUnusedLocal
    @staticmethod
    def _on_error(ws, error):
        logger.warning(f'WEBSOCKET: error: {error}.')

    # noinspection PyUnusedLocal
    @staticmethod
    def _on_close(ws):
        logger.warning('WEBSOCKET: feed closed.')

    # noinspection PyUnusedLocal
    def _on_open(self, ws):
        logger.info('WEBSOCKET: open feed.')
        topics = []
        if self.private:
            logger.info('WEBSOCKET: auth.')
            self.send_auth(ws)
            topics = self.private_topics
        else:
            if self.rest_api is not None:
                perp_list = [a['symbol'] for a in self.rest_api.fetch_perp_markets()]
            else:
                perp_list = PERP_LIST
            order_book_topics = [f'orderbook.{self.orderbook_depth}.{p}' for p in perp_list]
            ticker_topics = [f'tickers.{p}' for p in perp_list]
            if self.subscribe_to_tickers:
                logger.info('Subscribe to order books.')
                topics.extend(ticker_topics)
            if self.subscribe_to_order_books:
                logger.info('Subscribe to tickers.')
                topics.extend(order_book_topics)
        logger.info(f'WEBSOCKET: subscribe to {",".join(topics)}')
        self.ws.send(json.dumps({'op': 'subscribe', 'args': topics}))

    # noinspection PyUnusedLocal
    @staticmethod
    def _on_pong(*data):
        logger.debug('WEBSOCKET: pong received.')

    # noinspection PyUnusedLocal
    @staticmethod
    def _on_ping(ws, *data):
        logger.debug(f'WEBSOCKET: ping sent {datetime.now()}.')

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
            _runs_in_a_thread(self._run_forever, name='WS')
            self.wait_until_ready()
            logger.info('Websocket ready.')
        else:
            self._run_forever()

from __future__ import annotations

import sys
import typing
import warnings
from enum import Enum
from typing import Any, Self

import inflection
from pydantic import BaseModel
import requests

import config

class Exchange(str, Enum):
    Binance = "Binance"
    Okex = "Okex"
    Bybit = "Bybit"

    def other(self: Self) -> Exchange:
        res = {
            Exchange.Binance: Exchange.Okex,
            Exchange.Okex: Exchange.Binance,
        }
        return res[self]

    def slack_emoji(self: Self) -> str:
        res = {
            Exchange.Binance: ":binance:",
            Exchange.Okex: ":okx:",
            Exchange.Bybit: ":bybit:",
        }
        return res[self]

class Market(str, Enum):
    BinanceDelivery = "BinanceDelivery"
    BinanceFutures = "BinanceFutures"
    BinanceSpots = "BinanceSpots"
    OkexSpots = "OkexSpots"
    OkexSwaps = "OkexSwaps"
    BybitFutures = "BybitFutures"
    BybitInverse = "BybitInverse"
    BybitSpots = "BybitSpots"

    def __str__(self: Self) -> str:
        return self.value

    def to_exchange(self: Self) -> Exchange:
        exchange = {
            Market.BinanceDelivery: Exchange.Binance,
            Market.BinanceFutures: Exchange.Binance,
            Market.BinanceSpots: Exchange.Binance,
            Market.OkexSpots: Exchange.Okex,
            Market.OkexSwaps: Exchange.Okex,
            Market.BybitFutures: Exchange.Bybit,
            Market.BybitInverse: Exchange.Bybit,
            Market.BybitSpots: Exchange.Bybit,
        }
        return exchange[self]

    def endpoint_url(self: Self) -> str:
        endpoints = {
            Market.BinanceDelivery: "https://dapi.binance.com/dapi/v1",
            Market.BinanceFutures: "https://fapi.binance.com/fapi/v1",
            Market.BinanceSpots: "https://api.binance.com/api/v1",
            Market.OkexSpots: "https://www.okx.com/api/v5",
            Market.OkexSwaps: "https://www.okx.com/api/v5",
            Market.BybitFutures: "https://api.bybit.com/v5",
            Market.BybitInverse: "https://api.bybit.com/v5",
            Market.BybitSpots: "https://api.bybit.com/v5"
        }
        return endpoints[self]

    def fetch(self, path: str):
        url = self.endpoint_url() + path
        print(url, file=sys.stderr)
        with warnings.catch_warnings(action="ignore"):
            response = requests.get(url, verify=False)
        response.raise_for_status()
        json_data = response.json()
        return json_data

class Instrument(BaseModel):
    market: Market
    pair: str
    
    def convert(self: Self, other_market: Market) -> Instrument:
        if self.market == Market.BinanceFutures:
            if other_market == Market.BinanceFutures:
                return Instrument(market=other_market, pair=self.pair)
            elif other_market == Market.BinanceSpots:
                return Instrument(
                    market=other_market,
                    pair=self.pair
                    if not self.pair.startswith("1000")
                    else self.pair[4:],
                )
            elif other_market == Market.BinanceDelivery:
                if self.pair != "BTCUSDT" and self.pair != "ETHUSDT":
                    raise ValueError(
                        f"BinanceDelivery supports only BTC/ETH, not {self.pair}"
                    )
                return Instrument(
                    market=other_market,
                    pair=self.pair[:-1] + "_PERP",
                )
            elif other_market == Market.OkexSwaps:
                return Instrument(
                    market=other_market,
                    pair=(
                        self.pair if not self.pair.startswith("1000") else self.pair[4:]
                    ).split("USDT")[0]
                    + "-USDT-SWAP",
                )
            elif other_market == Market.OkexSpots:
                return Instrument(
                    market=other_market,
                    pair=(
                        self.pair if not self.pair.startswith("1000") else self.pair[4:]
                    ).split("USDT")[0]
                    + "-USDT",
                )
            else:
                raise ValueError(f"Unknown market {other_market}")
        else:
            raise ValueError("Can't convert not from BinanceFutures")

    def __str__(self) -> str:
        return f"{self.market.value}/{self.pair}"


from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, List, Optional, Self, Dict

from pydantic import BaseModel

import instrument
import config


def exchange_info_path(market: instrument.Market) -> str:
    paths = {
        instrument.Market.BinanceDelivery: "/exchangeInfo",
        instrument.Market.BinanceFutures: "/exchangeInfo",
        instrument.Market.BinanceSpots: "/exchangeInfo",
        instrument.Market.OkexSpots: "/public/instruments?instType=SPOT",
        instrument.Market.OkexSwaps: "/public/instruments?instType=SWAP",
        instrument.Market.BybitFutures: "/market/instruments-info?category=linear",
        instrument.Market.BybitInverse: "/market/instruments-info?category=inverse",
        instrument.Market.BybitSpots: "/market/instruments-info?category=spot",
    }
    return paths[market]


class BinanceExchangeInfoResponse(BaseModel):
    class Symbol(BaseModel):
        symbol: str
        filters: list[Any]
        status: Optional[str] = None
        contractStatus: Optional[str] = None
        onboardDate: Optional[int] = None

    symbols: List[Symbol]

    @staticmethod
    def convert(data: Any) -> BinanceExchangeInfoResponse:
        return config.from_dict(data, BinanceExchangeInfoResponse)


class OkexExchangeInfoResponse(BaseModel):
    class Symbol(BaseModel):
        instId: str
        state: str
        listTime: int

    data: List[Symbol]

    @staticmethod
    def convert(data: Any) -> OkexExchangeInfoResponse:
        return config.from_dict(data, OkexExchangeInfoResponse)


class BybitExchangeInfoResponse(BaseModel):
    class Symbol(BaseModel):
        symbol: str
        priceFilter: Dict[str, str]
        status: Optional[str] = None
        launchTime: Optional[int] = None

    list: List[Symbol]

    @staticmethod
    def convert(data: Any) -> BybitExchangeInfoResponse:
        return config.from_dict(data["result"], BybitExchangeInfoResponse)


class ExchangeInstrumentInfo(BaseModel):
    instr: instrument.Instrument
    status: str
    date: datetime

    def can_trade(self: Self) -> bool:
        statuses = {
            instrument.Exchange.Binance: "TRADING",
            instrument.Exchange.Okex: "live",
            instrument.Exchange.Bybit: "Trading",
        }
        return self.status == statuses[self.instr.market.to_exchange()]


class ExchangeInfo:
    Info = dict[str, ExchangeInstrumentInfo]

    market: instrument.Market
    info: Info

    def __init__(self: Self, market: instrument.Market):
        self.market = market
        data = self.market.fetch(exchange_info_path(self.market))
        match self.market.to_exchange():
            case instrument.Exchange.Binance:
                self.info = ExchangeInfo.convert_binance(
                    self.market, BinanceExchangeInfoResponse.convert(data)
                )
            case instrument.Exchange.Okex:
                self.info = ExchangeInfo.convert_okex(
                    self.market, OkexExchangeInfoResponse.convert(data)
                )
            case instrument.Exchange.Bybit:
                self.info = ExchangeInfo.convert_bybit(
                    self.market, BybitExchangeInfoResponse.convert(data)
                )

    @staticmethod
    def convert_binance(
        market: instrument.Market, data: BinanceExchangeInfoResponse
    ) -> Info:
        res: ExchangeInfo.Info = {}
        for v in data.symbols:
            date = datetime.fromtimestamp(0, UTC)
            if v.onboardDate is not None:
                date = datetime.fromtimestamp(v.onboardDate / 1000, UTC)
            status = "unknown"
            if v.status is not None:
                status = v.status
            elif v.contractStatus is not None:
                status = v.contractStatus
            res[v.symbol] = ExchangeInstrumentInfo(
                instr=instrument.Instrument(market=market, pair=v.symbol),
                status=status,
                date=date,
            )
        return res

    @staticmethod
    def convert_okex(market: instrument.Market, data: OkexExchangeInfoResponse) -> Info:
        res: ExchangeInfo.Info = {}
        for v in data.data:
            res[v.instId] = ExchangeInstrumentInfo(
                instr=instrument.Instrument(market=market, pair=v.instId),
                status=v.state,
                date=datetime.fromtimestamp(v.listTime / 1000, UTC),
            )
        return res

    @staticmethod
    def convert_bybit(
        market: instrument.Market, data: BybitExchangeInfoResponse
    ) -> Info:
        res: ExchangeInfo.Info = {}
        for v in data.list:
            date = datetime.fromtimestamp(0, UTC)
            if v.launchTime is not None:
                date = datetime.fromtimestamp(v.launchTime / 1000, UTC)
            status = "unknown"
            if v.status is not None:
                status = v.status
            res[v.symbol] = ExchangeInstrumentInfo(
                instr=instrument.Instrument(market=market, pair=v.symbol),
                status=status,
                date=date,
            )
        return res

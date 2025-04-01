import os
from datetime import datetime, timedelta, timezone

from pydantic import BaseModel
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from tap import Tap

import exchange_info
import instrument
import config


class ListingsArgs(Tap):
    channel: str
    test: bool = False


class Info(BaseModel):
    info: exchange_info.ExchangeInfo.Info
    market: instrument.Market

    @staticmethod
    def file_name(market: instrument.Market) -> str:
        return f"data_{market}.json"

    @classmethod
    def load(cls, market: instrument.Market):
        try:
            info = config.load(open(cls.file_name(market), "r"), Info)
            assert info.market == market
            return info
        except Exception as e:
            print(e)
            return None

    def save(self):
        config.dump(self, open(self.file_name(self.market), "w"))


def main():
    args = ListingsArgs(underscores_to_dashes=True).parse_args()
    slack_token = os.environ["SLACK_TOKEN"]

    markets = [
        instrument.Market.BinanceFutures,
        instrument.Market.BinanceSpots,
        instrument.Market.BinanceDelivery,
        instrument.Market.OkexSwaps,
        instrument.Market.OkexSpots,
        instrument.Market.BybitFutures,
        instrument.Market.BybitInverse,
        instrument.Market.BybitSpots,
    ]

    lists: list[tuple[Info, Info]] = []
    for market in markets:
        info = Info(info=exchange_info.ExchangeInfo(market).info, market=market)
        old = Info.load(market)
        if old is None:
            info.save()
            continue
        lists.append((info, old))

    # Initialize the Slack WebClient
    if (not args.test):
        client = WebClient(token=slack_token)

    not_updates = True

    def send_message(msg: str, exchange: instrument.Exchange | None):
        global not_updates
        if exchange is not None:
            msg = f"{exchange.slack_emoji()} {msg}"
        try:
            if (not args.test):
                response = client.chat_postMessage(channel=args.channel, text=msg)
                print("Message sent not succesfully:", response)
        except SlackApiError as e:
            print(f"Error sending message: {e.response['error']}")
            not_updates = False

    now = datetime.now(timezone.utc)
    for new, old in lists:
        for sym, info in old.info.items():
            name = str(info.instr)
            if sym not in new.info:
                msg = f"Old pair removed: {name}"
                send_message(msg, info.instr.market.to_exchange())
        for sym, info in new.info.items():
            name = str(info.instr)
            if sym not in old.info:
                msg = f"New pair added: {name}, status: {info.status}"
                send_message(msg, info.instr.market.to_exchange())
                if info.date > now:
                    send_message(
                        f"{name} will rollout at {info.date.strftime('%Y-%m-%d %H:%M:%S UTC')}",
                        info.instr.market.to_exchange(),
                    )
            else:
                old_info = old.info[sym]
                if info.status != old_info.status:
                    send_message(
                        f"Status changed: {name} was {old_info.status}, now {info.status}",
                        info.instr.market.to_exchange(),
                    )
            if info.date > now and info.date - now < timedelta(minutes=18):
                # if now - info.date < timedelta(days=2):
                send_message(
                    f"{name} will rollout in {info.date - now}",
                    info.instr.market.to_exchange(),
                )

    if not_updates:
        for new, _ in lists:
            new.save()
        try:
            text = f"Last updated at {now.strftime('%Y-%m-%d %H:%M:%S UTC')}"
            if (not args.test):
                client.chat_postMessage(channel=args.channel, text=text)
        except SlackApiError as e:
            print(f"Error sending message: {e.response['error']}")


if __name__ == "__main__":
    main()

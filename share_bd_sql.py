from tinkoff.invest import Client
from tinkoff.invest.schemas import InstrumentExchangeType
import my_token
from pprint import pprint
import sqlite3
from class_db import *

token = my_token.token
client_manager = Client(token)
client = client_manager.__enter__()


def merge(list1, list2, list3):
    merged_list = [(list1[i], list2[i], list3[i]) for i in range(0, len(list1))]

    return merged_list


def response():
    r = client.instruments.shares(
            instrument_exchange=InstrumentExchangeType.INSTRUMENT_EXCHANGE_UNSPECIFIED
        )
    figi_l = []
    ticker_l = []
    asset_uid_l = []

    for shares in r.instruments:
        if shares.class_code != 'TQBR':
            continue
        figi_l.append(shares.figi)
        ticker_l.append(shares.ticker)
        asset_uid_l.append(shares.asset_uid)
    sk = merge(ticker_l, figi_l, asset_uid_l)
    return sk


with db:
    data = response()
    DataShares.insert_many(data, fields=[DataShares.ticker, DataShares.figi, DataShares.asset_uid]).execute()
    print('y')

import requests
import json
import time
import datetime

import sys
import os
BASER_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(BASER_DIR)
DATABASE_DIR = os.path.join(BASER_DIR, "databases")
sys.path.append(DATABASE_DIR)

from login import Login
from databases.db_log import error, info
from databases.db_session import get_last_daily_data, get_all_security, get_historical_data, get_daily_data, get_lastest_tick_data
from databases.db_session import insert_object, insert_list_object
from databases.db_session import update_adj_price_data, update_lastest_tick_data, get_last_historical_data
from databases.db_model import HistoricalData, DailyData, LastestTickData

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

headers = {
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.fireant.vn",
    "Referer": "https://www.fireant.vn/App",
    "RequestVerificationToken": "DltKjjwpZAThh02U2YpFa92BFmcKUQooIEBS75iqMEF-8Csbvc9ZYMLiXd70R4ml0g0muLhkc32a0R0LenpmSZlH9RM1:NJLmVChZv88D3YV4GnYXC8tNzktGKXtHO-N4hF5XO_y5EVuDBVPczg6P04mN4hgt756X6s5yb0dGDNq__w74uxvHk7uZqLdaaxj-w7J83OcALUqPOimHgSluQ4ewnxuc2Jvn1w2",
    "Sec-Fetch-Mode": "cors",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36",
}
URL_DAILY_DATA_BASE = 'https://svr4.fireant.vn/api/Data/Companies/HistoricalQuotes?symbol={0}&startDate={1}&endDate={2}'
URL_DAILY_DATA_ADJ_BASE = 'https://svr4.fireant.vn/api/Data/Markets/HistoricalQuotes?symbol={0}&startDate={1}&endDate={2}'

URL_INVENTORY_BASE = 'https://svr1.fireant.vn/api/Data/Companies/CompanyInfo?symbol={0}'

def process_update_daily_data(sec_id, symbol, start_date, end_date):
    path = URL_DAILY_DATA_ADJ_BASE.format(symbol, start_date, end_date)
    response = requests.get(path, headers=headers)
    try:
        data = json.loads(response.text)
        for item in data:
            date = item.get("Date")
            if date is None:
                # xu ly lai
                continue
            fn_date = str(date)[:10]
            open = item.get("Open")
            close = item.get("Close")
            high = item.get("High")
            low = item.get("Low")
            update_adj_price_data(sec_id, fn_date, close, open, high, low)
    except Exception as e:
        error('error info: %s' % str(e))
        error('error at process_update_daily_data: %s' % symbol)
        error('========================***========================')


def process_daily_data(sec_id, symbol, start_date, end_date):
    path = URL_DAILY_DATA_BASE.format(symbol, start_date, end_date)
    path_inventory = URL_INVENTORY_BASE.format(symbol)
    response = requests.get(path, headers=headers)
    response_inventory = requests.get(path_inventory, headers=headers)
    try:
        data = json.loads(response.text)
        data_inventory = json.loads(response_inventory.text)
        historical_datas = []
        daily_datas = []
        is_adj = False
        for item in data:
            date = item.get("Date")
            price_open = item.get("PriceOpen")
            price_high = item.get("PriceHigh")
            price_low = item.get("PriceLow")
            price_close = item.get("PriceClose")
            adj_close = item.get("AdjClose")
            adj_open = item.get("AdjOpen")
            adj_high = item.get("AdjHigh")
            adj_low = item.get("AdjLow")

            if adj_close != price_close:
                is_adj = True

            deal_volume = item.get("DealVolume")
            put_through_volume = item.get("PutthroughVolume")
            total_volume = item.get("Volume")
            buy_foreign_quantity = item.get("BuyForeignQuantity")
            sell_foreign_quantity = item.get("SellForeignQuantity")
            buy_quantity = item.get("BuyQuantity")
            sell_quantity = item.get("SellQuantity")
            market_cap = item.get("MarketCap")

            state_ownership = data_inventory.get("StateOwnership")
            foreign_ownership = data_inventory.get("ForeignOwnership")
            other_ownership = data_inventory.get("OtherOwnership")

            if date is None:
                continue
            fn_date = str(date)[:10]

            historical_data = HistoricalData(sec_id, fn_date, price_open, price_high, price_low, price_close, adj_open, adj_high, adj_low, adj_close,
                                             deal_volume, put_through_volume, total_volume, datetime.datetime.now())
            daily_data = DailyData(sec_id, fn_date, buy_quantity, sell_quantity, buy_foreign_quantity,
                                   sell_foreign_quantity,
                                   market_cap, state_ownership, foreign_ownership, other_ownership)

            if len(symbol) == 3:
                trading_price = price_close / 1000
            else:
                trading_price = price_close
            lastest_tick_data = LastestTickData(sec_id, fn_date, trading_price, deal_volume, datetime.datetime.now())

            records = get_daily_data(sec_id, fn_date)
            if len(records) == 0:
                daily_datas.append(daily_data)
            else:
                info("Exist record in table daily_data with stock: {0}\t date: {1}".format(symbol, fn_date))

            records = get_historical_data(sec_id, fn_date)
            if len(records) == 0:
                historical_datas.append(historical_data)
            else:
                info("Exist record in table historical_data with stock: {0}\t date: {1}".format(symbol, fn_date))

            records = get_lastest_tick_data(sec_id, fn_date)
            if len(records) == 0:
                insert_object(lastest_tick_data)
            else:
                update_lastest_tick_data(sec_id, fn_date, price_close, deal_volume, datetime.datetime.now())

        insert_list_object(historical_datas)
        insert_list_object(daily_datas)
        if is_adj:
            process_update_daily_data(sec_id, symbol, '2008-01-01', end_date)
    except Exception as e:
        error('Exception %s' % str(e))
        error('process_daily_data: %s' % symbol)
        error('========================***========================')

def test_run():
    with open('C:\\TestService.log', 'a+') as f:
        f.write('test service running...\n')

def run():
    signup = Login()
    token = signup.get_token()
    if token is None:
        return
    headers['RequestVerificationToken'] = token

    today = datetime.date.today()
    print('Date: ', str(today))
    securities = get_all_security()
    for security in securities:
        symbol = security.name
        sec_id = security.id

        info('processing for symbol: {0}'.format(symbol))

        if sec_id == 1:
            symbol = 'HASTC'
        if sec_id == 2:
            symbol = 'HNX30'
        if sec_id == 3:
            symbol = 'HOSTC'
        if sec_id == 4:
            symbol = 'VN30'
        if sec_id == 6:
            symbol = 'VNXALL'
        time.sleep(0.2)

        records = get_last_daily_data(sec_id)
        if len(records) == 0:
            last_date_record = datetime.datetime.strptime("2008-01-01", "%Y-%m-%d").date()
        else:
            last_date_record = records[0].date

        next_day = last_date_record + datetime.timedelta(days=1)
        start_date = str(next_day)
        end_date = str(today)
        process_daily_data(sec_id, symbol, start_date, end_date)


    print('Done!')
    signup.close()

if __name__ == '__main__':
    run()
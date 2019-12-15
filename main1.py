import requests
import json
import time
import os
import datetime
import schedule
from login import Login

from databases.db_log import error, info
from databases.db_session import get_last_daily_data, get_all_security, insert_list_object, insert_object, \
    check_record_quarterly, check_record_yearly, check_record_historical, check_record_daily
from databases.db_session import check_record_business_plane, update_adj_price_data
from databases.db_model import HistoricalData, DailyData, QuarterlyData, YearlyData, BusinessPlanData

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
        info('done process_update_daily_data: {0}'.format(symbol))
    except Exception as e:
        error('error info: %s' % str(e))
        error('error at process_update_daily_data: %s' % symbol)
        error('========================***========================')


def process_daily_data(sec_id, symbol, start_date, end_date):
    path = URL_DAILY_DATA_BASE.format(symbol, start_date, end_date)
    path_adj = URL_DAILY_DATA_ADJ_BASE.format(symbol, start_date, end_date)
    response = requests.get(path, headers=headers)
    response_adj = requests.get(path_adj, headers=headers)
    try:
        data = json.loads(response.text)
        data_adj = json.loads(response_adj.text)
        historical_datas = {}
        results = []
        for item in data:
            date = item.get("Date")
            price_open = item.get("PriceOpen")
            price_high = item.get("PriceHigh")
            price_low = item.get("PriceLow")
            price_close = item.get("PriceClose")

            deal_volume = item.get("DealVolume")
            put_through_volume = item.get("PutthroughVolume")
            total_volume = item.get("Volume")

            if date is None:
                continue
            fn_date = str(date)[:10]

            records = check_record_daily(sec_id, fn_date)
            if len(records) > 0:
                info("Exist daily_data record with date: %s" % fn_date)
                continue
            historical_datas[fn_date] = [price_open, price_high, price_low, price_close, deal_volume, put_through_volume, total_volume]

        for item in data_adj:
            date = item.get("Date")
            if date is None:
                # xu ly lai
                continue
            fn_date = str(date)[:10]
            open = item.get("Open")
            close = item.get("Close")
            high = item.get("High")
            low = item.get("Low")
            data1 = historical_datas.get(fn_date)
            if data1:
                data1.append(open)
                data1.append(high)
                data1.append(low)
                data1.append(close)
        for k, v in historical_datas.items():
            historical_data = HistoricalData(sec_id, k, v[0], v[1], v[2], v[3], v[7],
                                             v[8], v[9], v[10],
                                             v[4], v[5], v[6], datetime.datetime.now())
            results.append(historical_data)
        insert_list_object(results)
        info('done process_daily_data: {0}'.format(symbol))
    except Exception as e:
        error('Exception %s' % str(e))
        error('process_daily_data: %s' % symbol)
        error('========================***========================')



def run():
    signup = Login()
    token = signup.get_token()
    if token is None:
        return
    headers['RequestVerificationToken'] = token

    today = datetime.date.today()
    print('Update data at: ', str(today))
    securities = get_all_security()
    for security in securities:
        symbol = security.name
        sec_id = security.id
        if sec_id == 3:
            symbol = 'HOSTC'
        time.sleep(0.2)

        last_date_record = datetime.datetime.strptime("2008-01-01", "%Y-%m-%d").date()

        start_date = str(last_date_record)
        end_date = str(today)
        process_daily_data(sec_id, symbol, start_date, end_date)

    print('Done!')
    signup.close()


if __name__ == '__main__':
    run()
    # schedule.every().day.at("17:00").do(run)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)

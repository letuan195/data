import requests
import json
import time
import datetime
import schedule
from login import Login

import sys
import os
BASER_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(BASER_DIR)
DATABASE_DIR = os.path.join(BASER_DIR, "databases")
sys.path.append(DATABASE_DIR)

from databases.db_log import error, info
from databases.db_session import get_yearly_data, get_quarterly_data, get_business_plan
from databases.db_session import get_last_daily_data, get_all_security, get_historical_data, get_daily_data, get_lastest_tick_data
from databases.db_session import insert_list_object, insert_object
from databases.db_session import update_adj_price_data, update_lastest_tick_data
from databases.db_model import HistoricalData, DailyData, QuarterlyData, YearlyData, BusinessPlanData, LastestTickData

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

URL_BASE_QUARTER = 'https://svr4.fireant.vn/api/Data/Finance/QuarterlyFinancialInfo?symbol={0}&fromYear={1}&fromQuarter={2}&toYear={3}&toQuarter={4}'
URL_BASE_YEAR = 'https://svr4.fireant.vn/api/Data/Finance/YearlyFinancialInfo?symbol={0}&fromYear={1}&toYear={2}'

URL_INVENTORY_BASE = 'https://svr1.fireant.vn/api/Data/Companies/CompanyInfo?symbol={0}'

URL_BUSINESS_PLAN_BASE = 'http://e.cafef.vn/khkd.ashx?symbol={0}'


# URL_BCTC_YEAR_BASE = 'https://svr2.fireant.vn/api/Data/Finance/LastestFinancialReports?symbol={0}&type={1}&year={2}&quarter=0&count={3}'

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
            # lastest_tick_data = LastestTickData(sec_id, fn_date, price_close / 1000, deal_volume, datetime.datetime.now())

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

            # records = get_lastest_tick_data(sec_id, fn_date)
            # if len(records) == 0:
            #     insert_object(lastest_tick_data)
            # else:
            #     update_lastest_tick_data(sec_id, fn_date, price_close / 1000, deal_volume, datetime.datetime.now())

        insert_list_object(historical_datas)
        insert_list_object(daily_datas)
        if is_adj:
            process_update_daily_data(sec_id, symbol, '2008-01-01', end_date)
    except Exception as e:
        error('Exception %s' % str(e))
        error('process_daily_data: %s' % symbol)
        error('========================***========================')


def process_quarterly_data(sec_id, symbol, start_year, start_quarter, end_year, end_quarter):
    path = URL_BASE_QUARTER.format(symbol, start_year, start_quarter, end_year, end_quarter)
    response = requests.get(path, headers=headers)
    try:
        data = json.loads(response.text)
        for year in range(start_year, end_year + 1):
            for quarter in range(start_quarter, end_quarter + 1):
                records = get_quarterly_data(sec_id, year, quarter)
                if len(records) > 0:
                    continue
                for item in data:
                    year_crawl = item.get("Year")
                    quarter_crawl = item.get("Quarter")
                    roe_crawl = item.get("ROE_TTM")
                    roa_crawl = item.get("ROA_TTM")
                    roic_crawl = item.get("ROIC_TTM")
                    eps_crawl = item.get("BasicEPS_MRQ")
                    cash_crawl = item.get("Cash_MRQ")
                    assets_crawl = item.get("TotalAssets_MRQ")
                    equity_crawl = item.get("Equity_MRQ")
                    profit_after_tax_crawl = item.get("ProfitAfterTax_MRQ")
                    profit_before_tax_crawl = item.get("ProfitBeforeTax_MRQ")
                    revenue_crawl = item.get("NetSales_MRQ")
                    liabilities_crawl = item.get("Liabilities_MRQ")
                    current_liabilities_crawl = item.get("CurrentLiabilities_MRQ")
                    bookvalue_per_share_crawl = item.get("BookValuePerShare_MRQ")
                    sales_per_share_crawl = item.get("SalesPerShare_TTM")
                    if year == year_crawl and quarter == quarter_crawl:
                        data_db = QuarterlyData(sec_id, year_crawl, quarter_crawl, roe_crawl, roa_crawl, roic_crawl,
                                                eps_crawl,
                                                cash_crawl, assets_crawl, equity_crawl, profit_after_tax_crawl,
                                                profit_before_tax_crawl, revenue_crawl, liabilities_crawl,
                                                current_liabilities_crawl, bookvalue_per_share_crawl,
                                                sales_per_share_crawl)
                        insert_object(data_db)
                        break

                temp_year = year
                temp_quarter = quarter - 2
                if quarter == 1:
                    temp_year = year - 1
                    temp_quarter = 3
                elif quarter == 2:
                    temp_year = year - 1
                    temp_quarter = 4
                records = get_quarterly_data(sec_id, temp_year, temp_quarter)
                if len(records) == 0:
                    data_db = QuarterlyData(sec_id, temp_year, temp_quarter, None, None, None, None, None, None, None,
                                            None, None, None, None, None, None, None)
                    insert_object(data_db)
    except Exception as e:
        error('Exception %s' % str(e))
        error('process_quarterly_data: %s' % symbol)
        error('========================***========================')


def process_yearly_data(sec_id, symbol, start_year, end_year):
    path = URL_BASE_YEAR.format(symbol, start_year, end_year)
    response = requests.get(path, headers=headers)
    try:
        data = json.loads(response.text)
        for year in range(start_year - 1, end_year):
            records = get_yearly_data(sec_id, year)
            if len(records) > 0:
                continue
            for item in data:
                year_crawl = item.get("Year")
                roe_crawl = item.get("ROE")
                roa_crawl = item.get("ROA")
                roic_crawl = item.get("ROIC")
                eps_crawl = item.get("BasicEPS")
                cash_crawl = item.get("Cash")
                assets_crawl = item.get("TotalAssets")
                equity_crawl = item.get("Equity")
                profit_after_tax_crawl = item.get("ProfitAfterTax")
                profit_before_tax_crawl = item.get("ProfitBeforeTax")
                revenue_crawl = item.get("Sales")
                liabilities_crawl = item.get("Liabilities")
                current_liabilities_crawl = item.get("CurrentLiabilities")
                bookvalue_per_share_crawl = item.get("BookValuePerShare")
                sales_per_share_crawl = item.get("SalesPerShare")
                if year == year_crawl:
                    data_db = YearlyData(sec_id, year_crawl, roe_crawl, roa_crawl, roic_crawl, eps_crawl, cash_crawl,
                                         assets_crawl, equity_crawl, profit_after_tax_crawl,
                                         profit_before_tax_crawl, revenue_crawl, liabilities_crawl,
                                         current_liabilities_crawl, bookvalue_per_share_crawl,
                                         sales_per_share_crawl)
                    insert_object(data_db)
                    break
            temp_year = year - 2
            records = get_yearly_data(sec_id, temp_year)
            if len(records) == 0:
                data_db = YearlyData(sec_id, temp_year, None, None, None, None, None, None, None, None, None, None,
                                     None, None, None, None)
                insert_object(data_db)
    except Exception as e:
        error('Exception %s' % str(e))
        error('process_yearly_data: %s' % symbol)
        error('========================***========================')


def process_business_plan_data(sec_id, symbol):
    path = URL_BUSINESS_PLAN_BASE.format(symbol)
    response = requests.get(path)
    try:
        data = json.loads(response.text)
        if data is None:
            return
        for item in data:
            year_crawl = item.get('KYear')
            total_income_crawl = item.get('Totalncome')
            profit_crawl = item.get('TotalProfit')
            net_income_crawl = item.get('Netlncome')
            dividend_stock_crawl = item.get('DivStock')
            dividend_money_crawl = item.get('Dividend')
            records = get_business_plan(sec_id, year_crawl)
            if len(records) == 0:
                x = 1000000000
                business = BusinessPlanData(sec_id, year_crawl, total_income_crawl * x, profit_crawl * x,
                                            net_income_crawl * x, dividend_stock_crawl, dividend_money_crawl)
                insert_object(business)
    except Exception as e:
        error('Exception %s' % str(e))
        error('process_business_plan_data: %s' % symbol)
        error('========================***========================')


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
            start_year = 2008
            start_quarter = 1
        else:
            last_date_record = records[0].date
            start_year = int(last_date_record.year)
            start_quarter = int((last_date_record.month - 1) / 3) + 1

        next_day = last_date_record + datetime.timedelta(days=1)
        start_date = str(next_day)
        end_date = str(today)
        process_daily_data(sec_id, symbol, start_date, end_date)

        end_year = int(today.year)
        end_quarter = 4
        for year in range(start_year, end_year + 1):
            process_quarterly_data(sec_id, symbol, year, start_quarter, year, end_quarter)

        process_yearly_data(sec_id, symbol, start_year, end_year)
        process_business_plan_data(sec_id, symbol)

    print('Done!')
    signup.close()


if __name__ == '__main__':
    run()

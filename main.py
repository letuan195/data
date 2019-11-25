import requests
import json
import time
import os
import datetime
import schedule
from login import Login

from databases.db_log import error
from databases.db_session import get_all_security, insert_list_object, insert_object, check_record_quarterly, check_record_yearly, check_record_historical, check_record_daily
from databases.db_session import check_record_business_plane
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

URL_BASE_QUARTER = 'https://svr4.fireant.vn/api/Data/Finance/QuarterlyFinancialInfo?symbol={0}&fromYear={1}&fromQuarter={2}&toYear={3}&toQuarter={4}'
URL_BASE_YEAR = 'https://svr4.fireant.vn/api/Data/Finance/YearlyFinancialInfo?symbol={0}&fromYear={1}&toYear={2}'

URL_INVENTORY_BASE = 'https://svr1.fireant.vn/api/Data/Companies/CompanyInfo?symbol={0}'

URL_BUSINESS_PLAN_BASE = 'http://e.cafef.vn/khkd.ashx?symbol={0}'

# URL_BCTC_YEAR_BASE = 'https://svr2.fireant.vn/api/Data/Finance/LastestFinancialReports?symbol={0}&type={1}&year={2}&quarter=0&count={3}'

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
        for item in data:
            date = item.get("Date")
            if date is None:
                continue
            fn_date = str(date)[:10]

            records = check_record_historical(sec_id, fn_date)
            if len(records) > 0:
                continue

            price_open = item.get("PriceOpen")
            price_close = item.get("PriceClose")
            price_high = item.get("PriceHigh")
            price_low = item.get("PriceLow")

            volume = item.get("Volume")
            buy_foreign_quantity = item.get("BuyForeignQuantity")
            sell_foreign_quantity = item.get("SellForeignQuantity")
            buy_quantity = item.get("BuyQuantity")
            sell_quantity = item.get("SellQuantity")
            market_cap = item.get("MarketCap")

            state_ownership = data_inventory.get("StateOwnership")
            foreign_ownership = data_inventory.get("ForeignOwnership")
            other_ownership = data_inventory.get("OtherOwnership")



            historical_data = HistoricalData(sec_id, fn_date, price_open, price_high, price_low, price_close, volume, datetime.date.today())
            daily_data = DailyData( sec_id, fn_date, buy_quantity, sell_quantity, buy_foreign_quantity, sell_foreign_quantity,
                                    market_cap, state_ownership, foreign_ownership, other_ownership)
            historical_datas.append(historical_data)
            daily_datas.append(daily_data)
        insert_list_object(historical_datas)
        insert_list_object(daily_datas)
    except Exception as e:
        error('========================***========================')
        error('error info: %s' % str(e))
        error('error at process_daily_data: %s' % symbol)
        error('========================***========================')


def process_quarterly_data(sec_id, symbol, start_year, start_quarter, end_year, end_quarter):
    path = URL_BASE_QUARTER.format(symbol, start_year, start_quarter, end_year, end_quarter)
    response = requests.get(path, headers=headers)
    try:
        data = json.loads(response.text)
        for year in range(start_year, end_year + 1):
            for quarter in range(start_quarter, end_quarter + 1):
                records = check_record_quarterly(sec_id, year, quarter)
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
                    if year==year_crawl and quarter==quarter_crawl:
                        data_db = QuarterlyData(sec_id, year_crawl, quarter_crawl, roe_crawl, roa_crawl, roic_crawl, eps_crawl,
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
                records = check_record_quarterly(sec_id, temp_year, temp_quarter)
                if len(records) == 0:
                    data_db = QuarterlyData(sec_id, temp_year, temp_quarter, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
                    insert_object(data_db)
    except Exception as e:
        error('========================***========================')
        error('error info: %s' % str(e))
        error('error at process_quarterly_data: %s' % symbol)
        error('========================***========================')


def process_yearly_data(sec_id, symbol, start_year, end_year):
    path = URL_BASE_YEAR.format(symbol, start_year, end_year)
    response = requests.get(path, headers=headers)
    try:
        data = json.loads(response.text)
        for year in range(start_year, end_year):
            records = check_record_yearly(sec_id, year)
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
                if year==year_crawl:
                    data_db = YearlyData(sec_id, year_crawl, roe_crawl, roa_crawl, roic_crawl, eps_crawl, cash_crawl, assets_crawl, equity_crawl, profit_after_tax_crawl,
                                         profit_before_tax_crawl, revenue_crawl, liabilities_crawl, current_liabilities_crawl, bookvalue_per_share_crawl,
                                         sales_per_share_crawl)
                    insert_object(data_db)
                    break
            temp_year = year - 2
            records = check_record_yearly(sec_id, temp_year)
            if len(records) == 0:
                data_db = YearlyData(sec_id, temp_year, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
                insert_object(data_db)
    except Exception as e:
        error('========================***========================')
        error('error info: %s' % str(e))
        error('error at process_yearly_data: %s'% symbol)
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
            records = check_record_business_plane(sec_id, year_crawl)
            if len(records) == 0:
                x = 1000000000
                business = BusinessPlanData(sec_id, year_crawl, total_income_crawl * x, profit_crawl * x, net_income_crawl * x, dividend_stock_crawl, dividend_money_crawl)
                insert_object(business)

    except Exception as e:
        error('========================***========================')
        error('error info: %s' % str(e))
        error('error at process_business_plan_data: %s'% symbol)
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

        # start_date = str(today)
        start_date = str(today)
        end_date = str(today)
        process_daily_data(sec_id, symbol, start_date, end_date)

        start_year = int(today.year)
        # start_year = 2008
        start_quarter = int((today.month - 1)/3) + 1
        # start_quarter = 1
        end_year = int(today.year)
        end_quarter = 4
        for year in range(start_year, end_year + 1):
            process_quarterly_data(sec_id, symbol, year, start_quarter, year, end_quarter)

        start_year = int(today.year) - 1
        # start_year = 2008
        end_year = int(today.year)
        process_yearly_data(sec_id, symbol, start_year, end_year)

        process_business_plan_data(sec_id, symbol)

    print('\t\t\t Done!')
    signup.close()

if __name__ == '__main__':
    # run()
    schedule.every().day.at("17:00").do(run)
    while True:
        schedule.run_pending()
        time.sleep(1)

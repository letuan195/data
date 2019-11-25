import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Unicode, Integer, DateTime, BigInteger, Date, DECIMAL, VARCHAR, INT
from sqlalchemy.dialects.mysql import DOUBLE, DATETIME, TINYINT

BASER_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(BASER_DIR)

Base = declarative_base()
ACCESS_FILE_PATH = os.path.dirname(os.path.abspath(__file__)) + '/access.txt'


class HistoricalData(Base):
    __tablename__ = 'historical_data'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    sec_id = Column(BigInteger, nullable=False)
    date = Column(Date, nullable=False)
    open = Column(DECIMAL(20, 4), nullable=True)
    high = Column(DECIMAL(20, 4), nullable=True)
    low = Column(DECIMAL(20, 4), nullable=True)
    close = Column(DECIMAL(20, 4), nullable=True)
    volume = Column(DECIMAL(20, 0), nullable=True)
    last_update = Column(Date, nullable=False)

    def __init__(self, sec_id, date, open, high, low, close, volume, last_update):
        self.sec_id = sec_id
        self.date = date
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.last_update = last_update


class Security(Base):
    __tablename__ = 'security'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    code = Column(VARCHAR(50))
    name = Column(VARCHAR(100))
    full_name = Column(VARCHAR(250))
    currency = Column(VARCHAR(10))
    sec_type = Column(INT)
    group_type = Column(INT)
    min_price_increment = Column(DECIMAL(10, 0))
    min_unit_increment = Column(DECIMAL(10, 0))
    number_of_decimal = Column(INT)
    price_multiplier = Column(DECIMAL(10, 0))
    unit_multiplier = Column(DECIMAL(10, 0))
    status = Column(INT)
    exchange_id = Column(INT)

    def __init__(self, code, name, full_name, currency, sec_type, group_type, min_price_increment, min_unit_increment,
                 number_of_decimal, price_multiplier, unit_multiplier, status, exchange_id):
        self.code = code
        self.name = name
        self.full_name = full_name
        self.currency = currency
        self.sec_type = sec_type
        self.group_type = group_type
        self.min_price_increment = min_price_increment
        self.min_unit_increment = min_unit_increment
        self.number_of_decimal = number_of_decimal
        self.price_multiplier = price_multiplier
        self.unit_multiplier = unit_multiplier
        self.status = status
        self.exchange_id = exchange_id


class SecurityGroup(Base):
    __tablename__ = 'security_group'
    id = Column(INT, primary_key=True, autoincrement=True)
    name = Column(VARCHAR(500))
    description = Column(VARCHAR(1000))

    def __init__(self, name, description):
        self.description = description
        self.name = name


class SecurityType(Base):
    __tablename__ = 'security_type'
    id = Column(INT, primary_key=True, autoincrement=True)
    name = Column(VARCHAR(250))
    description = Column(VARCHAR(1000))

    def __init__(self, name, description):
        self.description = description
        self.name = name


class Exchange(Base):
    __tablename__ = 'exchange'
    id = Column(INT, primary_key=True, autoincrement=True)
    name = Column(VARCHAR(100))
    full_name = Column(VARCHAR(250))
    country = Column(VARCHAR(3))
    opening_time = Column(DATETIME)
    closing_time = Column(DATETIME)
    status = Column(INT)
    state = Column(INT)
    use_dts = Column(TINYINT)

    def __init__(self, name, full_name, country, opening_time, closing_time, status, state, use_dts):
        self.full_name = full_name
        self.name = name
        self.country = country
        self.opening_time = opening_time
        self.closing_time = closing_time
        self.status = status
        self.state = state
        self.use_dts = use_dts


class DailyData(Base):
    __tablename__ = 'daily_data'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    sec_id = Column(BigInteger, nullable=False)
    date = Column(Date, nullable=False)
    buy_quantity = Column(DECIMAL(20, 0), nullable=True)
    sell_quantity = Column(DECIMAL(20, 0), nullable=True)
    foreign_buy_volume = Column(DECIMAL(20, 0), nullable=True)
    foreign_sell_volume = Column(DECIMAL(20, 0), nullable=True)
    cap = Column(DECIMAL(20, 0), nullable=True)
    state_ownership = Column(DOUBLE, nullable=True)
    foreign_ownership = Column(DOUBLE, nullable=True)
    other_ownership = Column(DOUBLE, nullable=True)

    def __init__(self, sec_id, date, buy_quantity, sell_quantity, foreign_buy_volume, foreign_sell_volume,
                 cap, state_ownership, foreign_ownership, other_ownership):
        self.sec_id = sec_id
        self.date = date
        self.buy_quantity = buy_quantity
        self.sell_quantity = sell_quantity
        self.foreign_buy_volume = foreign_buy_volume
        self.foreign_sell_volume = foreign_sell_volume
        self.cap = cap
        self.state_ownership = state_ownership
        self.foreign_ownership = foreign_ownership
        self.other_ownership = other_ownership


class QuarterlyData(Base):
    __tablename__ = 'quarterly_data'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    sec_id = Column(BigInteger, nullable=False)
    year = Column(INT, nullable=False)
    quarter = Column(INT, nullable=False)
    eps = Column(DOUBLE, nullable=True)
    roa = Column(DOUBLE, nullable=True)
    roe = Column(DOUBLE, nullable=True)
    roic = Column(DOUBLE, nullable=True)
    cash = Column(DECIMAL(20, 0), nullable=True)
    assets = Column(DECIMAL(20, 0), nullable=True)
    equity = Column(DECIMAL(20, 0), nullable=True)
    profit_after_tax = Column(DECIMAL(20, 0), nullable=True)
    profit_before_tax = Column(DECIMAL(20, 0), nullable=True)
    revenue = Column(DECIMAL(20, 0), nullable=True)
    liabilities = Column(DECIMAL(20, 0), nullable=True)
    current_liabilities = Column(DECIMAL(20, 0), nullable=True)
    bookvalue_per_share = Column(DECIMAL(20, 0), nullable=True)
    sales_per_share = Column(DECIMAL(20, 0), nullable=True)

    def __init__(self, sec_id, year, quarter, roe, roa, roic, eps, cash, assets, equity, profit_after_tax, profit_before_tax,
                 revenue, liabilities, current_liabilities, bookvalue_per_share, sales_per_share):
        self.sec_id = sec_id
        self.year = year
        self.quarter = quarter
        self.eps = eps
        self.roa = roa
        self.roe = roe
        self.roic = roic
        self.cash = cash
        self.assets = assets
        self.equity = equity
        self.profit_after_tax = profit_after_tax
        self.profit_before_tax = profit_before_tax
        self.revenue = revenue
        self.liabilities = liabilities
        self.current_liabilities = current_liabilities
        self.bookvalue_per_share = bookvalue_per_share
        self.sales_per_share = sales_per_share


class YearlyData(Base):
    __tablename__ = 'yearly_data'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    sec_id = Column(BigInteger, nullable=False)
    year = Column( INT, nullable=False)
    eps = Column(DOUBLE, nullable=True)
    roa = Column(DOUBLE, nullable=True)
    roe = Column(DOUBLE, nullable=True)
    roic = Column(DOUBLE, nullable=True)
    cash = Column(DECIMAL(20, 0), nullable=True)
    assets = Column(DECIMAL(20, 0), nullable=True)
    equity = Column(DECIMAL(20, 0), nullable=True)
    profit_after_tax = Column(DECIMAL(20, 0), nullable=True)
    profit_before_tax = Column(DECIMAL(20, 0), nullable=True)
    revenue = Column(DECIMAL(20, 0), nullable=True)
    liabilities = Column(DECIMAL(20, 0), nullable=True)
    current_liabilities = Column(DECIMAL(20, 0), nullable=True)
    bookvalue_per_share = Column(DECIMAL(20, 0), nullable=True)
    sales_per_share = Column(DECIMAL(20, 0), nullable=True)

    def __init__(self, sec_id, year, roe, roa, roic, eps, cash, assets, equity, profit_after_tax, profit_before_tax, revenue,
                 liabilities, current_liabilities, bookvalue_per_share, sales_per_share):
        self.sec_id = sec_id
        self.year = year
        self.eps = eps
        self.roa = roa
        self.roe = roe
        self.roic = roic
        self.cash = cash
        self.assets = assets
        self.equity = equity
        self.profit_after_tax = profit_after_tax
        self.profit_before_tax = profit_before_tax
        self.revenue = revenue
        self.liabilities = liabilities
        self.current_liabilities = current_liabilities
        self.bookvalue_per_share = bookvalue_per_share
        self.sales_per_share = sales_per_share


class BusinessPlanData(Base):
    __tablename__ = 'business_plan_data'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    sec_id = Column(BigInteger, nullable=False)
    year = Column( INT, nullable=False)
    total_income = Column(DECIMAL(20, 0), nullable=True)
    profit = Column(DECIMAL(20, 0), nullable=True)
    net_income = Column(DECIMAL(20, 0), nullable=True)
    dividend_stock = Column(DECIMAL(20, 0), nullable=True)
    dividend_money = Column(DECIMAL(20, 0), nullable=True)

    def __init__(self, sec_id, year, total_income, profit, net_income, dividend_stock, dividend_money):
        self.sec_id = sec_id
        self.year = year
        self.total_income = total_income
        self.profit = profit
        self.net_income = net_income
        self.dividend_stock = dividend_stock
        self.dividend_money = dividend_money


def read_file_config():
    result = {}
    f = open(ACCESS_FILE_PATH, 'r')
    for line in f:
        temp = line.split(':')
        result[temp[0].strip()] = temp[1].strip()
    f.close()
    return result

CONFIG_DIC = read_file_config()


def get_engine():
    user_name = CONFIG_DIC['user']
    password = CONFIG_DIC['password']
    host = CONFIG_DIC['host']
    db_name = CONFIG_DIC['database']
    mysql_engine_str = 'mysql+mysqldb://%s:%s@%s/%s?charset=utf8' % (user_name, password, host, db_name)
    engine = create_engine(mysql_engine_str, pool_recycle=3600 * 7)
    return engine


def create_database():
    engine = get_engine()
    Base.metadata.create_all(engine)

# if __name__ == '__main__':
#     create_database()

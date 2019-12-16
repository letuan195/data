from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import desc, or_

from db_model import get_engine, Security, QuarterlyData, YearlyData, DailyData, HistoricalData, BusinessPlanData, LastestTickData
from db_log import error

Base = declarative_base()
ENGINE = get_engine()
Base.metadata.bind = ENGINE
DBSession = sessionmaker(bind=ENGINE)


def insert_object(object):
    session = DBSession()
    try:
        session.add(object)
        session.commit()
    except Exception as e:
        error('error at inserting object')
        error('error info: %s' % str(e))
        session.rollback()
    finally:
        session.close()


def insert_list_object(list_object):
    session = DBSession()
    try:
        session.bulk_save_objects(list_object)
        session.commit()
    except Exception as e:
        error('error at inserting %d objects' % len(list_object))
        error('error info: %s' % str(e))
        session.rollback()
    finally:
        session.close()


def get_last_daily_data(sec_id):
    session = DBSession()
    try:
        query = session.query(DailyData).filter_by(sec_id=sec_id).order_by(desc(DailyData.date)).limit(1)
        records = query.all()
    except Exception as e:
        error('========================***========================')
        error('error info: %s' % str(e))
        error('error at check_record_historical')
        error('========================***========================')
        records = []
    finally:
        session.close()
    return records


def get_last_historical_data(sec_id):
    session = DBSession()
    try:
        query = session.query(HistoricalData).filter_by(sec_id=sec_id).order_by(desc(HistoricalData.date)).limit(1)
        records = query.all()
    except Exception as e:
        error('========================***========================')
        error('error info: %s' % str(e))
        error('error at get_last_historical_data')
        error('========================***========================')
        records = []
    finally:
        session.close()
    return records


def get_historical_data(sec_id, date):
    session = DBSession()
    try:
        query = session.query(HistoricalData).filter_by(sec_id=sec_id, date=date)
        records = query.all()
    except Exception as e:
        error('========================***========================')
        error('error info: %s' % str(e))
        error('error at check_record_historical')
        error('========================***========================')
        records = []
    finally:
        session.close()
    return records


def get_daily_data(sec_id, date):
    session = DBSession()
    try:
        query = session.query(DailyData).filter_by(sec_id=sec_id, date=date)
        records = query.all()
    except Exception as e:
        error('========================***========================')
        error('error info: %s' % str(e))
        error('error at check_record_daily')
        error('========================***========================')
        records = []
    finally:
        session.close()
    return records


def get_quarterly_data(sec_id, year, quarter):
    session = DBSession()
    try:
        query = session.query(QuarterlyData).filter_by(sec_id=sec_id, year=year, quarter=quarter)
        records = query.all()
    except Exception as e:
        error('========================***========================')
        error('error info: %s' % str(e))
        error('error at check_record_quater')
        error('========================***========================')
        records = []
    finally:
        session.close()
    return records


def get_yearly_data(sec_id, year):
    session = DBSession()
    try:
        query = session.query(YearlyData).filter_by(sec_id=sec_id, year=year)
        records = query.all()
    except Exception as e:
        error('========================***========================')
        error('error info: %s' % str(e))
        error('error at check_record_yearly')
        error('========================***========================')
        records = []
    finally:
        session.close()
    return records


def get_business_plan(sec_id, year):
    session = DBSession()
    try:
        query = session.query(BusinessPlanData).filter_by(sec_id=sec_id, year=year)
        records = query.all()
    except Exception as e:
        error('========================***========================')
        error('error info: %s' % str(e))
        error('error at check_record_business_plane')
        error('========================***========================')
        records = []
    finally:
        session.close()
    return records

def get_lastest_tick_data(sec_id, trading_date):
    session = DBSession()
    try:
        query = session.query(LastestTickData).filter_by(sec_id=sec_id, trading_date=trading_date)
        records = query.all()
    except Exception as e:
        error('========================***========================')
        error('error info: %s' % str(e))
        error('error at get_lastest_tick_data')
        error('========================***========================')
        records = []
    finally:
        session.close()
    return records


def get_all_security():
    session = DBSession()
    try:
        # query = session.query(Security).filter_by(title=title, subtitle=subtitle, link=link, news_type=news_type)
        query = session.query(Security)
        records = query.all()
        # if len(records) < 1:
        #     print('article is not exist')
    except Exception as e:
        error('========================***========================')
        error('error info: %s' % str(e))
        error('error at get_all_security')
        error('========================***========================')
        records = []
    finally:
        session.close()
    return records


def update_adj_price_data(sec_id, date, close, price_open, high, low):
    session = DBSession()
    try:
        session.query(HistoricalData) \
            .filter(HistoricalData.sec_id == sec_id) \
            .filter(HistoricalData.date == date). \
            update({"adj_close": close, "adj_open": price_open, "adj_high": high, "adj_low": low})
        session.commit()
    except Exception as e:
        error('error at update_adj_price_data object')
        error('error info: %s' % str(e))
        session.rollback()
    finally:
        session.close()


def update_lastest_tick_data(sec_id, trading_date, trading_price, total_volume, last_update):
    session = DBSession()
    try:
        session.query(LastestTickData) \
            .filter(LastestTickData.sec_id == sec_id) \
            .filter(LastestTickData.trading_date == trading_date). \
            update({"trading_price": trading_price, "total_volume": total_volume, "last_update": last_update})
        session.commit()
    except Exception as e:
        error('error at update_lastest_tick_data object')
        error('error info: %s' % str(e))
        session.rollback()
    finally:
        session.close()

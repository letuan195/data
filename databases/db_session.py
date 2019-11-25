from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import asc, or_

import os
import sys

BASER_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(BASER_DIR)

from db_model import get_engine, Security, QuarterlyData, YearlyData, DailyData, HistoricalData, BusinessPlanData
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


def check_record_historical(sec_id, date):
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


def check_record_daily(sec_id, date):
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


def check_record_quarterly(sec_id, year, quarter):
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


def check_record_yearly(sec_id, year):
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

def check_record_business_plane(sec_id, year):
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


# def update_daily_data():
#     session = DBSession()
#     try:
#         session.query(DailyData).update()
#         session.commit()
#     except Exception as e:
#         error('error at update_daily_data object')
#         error('error info: %s' % str(e))
#         session.rollback()
#     finally:
#         session.close()

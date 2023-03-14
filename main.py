import os
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import configparser
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.sqlite import insert
import bls

from models import *

#logging.basicConfig(level=logging.INFO)

api_key_filename = "api_key.ini"
logging.info("Reading config file.")
user_config = configparser.RawConfigParser()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
user_config.read(os.path.join(BASE_DIR ,api_key_filename))
bls_api_key = user_config.get('api_key', 'BLS_API_KEY')

def connect_to_db(db_filename):
    # Connect to the database using SQLAlchemy
    url = 'sqlite:///{}'.format(db_filename)
    engine = create_engine(url, echo=False)
    Session = sessionmaker()
    Session.configure(bind=engine, autoflush=False)
    session = Session()
    return engine, session

def str_replace(s):
    if (s[-2:]) == 'Q1':
        return s[:-2] + '-03'
    elif (s[-2:]) == 'Q2':
        return s[:-2] + '-06'
    elif (s[-2:]) == 'Q3':
        return s[:-2] + '-09'
    elif (s[-2:]) == 'Q4':
        return s[:-2] + '-12'
    else:
        return None

def download_data_to_sql(series_id, startyear,endyear, api_key, sql_table_name, engine):
    df = pd.DataFrame(bls.get_series(series_id, startyear, endyear, api_key))
    logging.info(df.columns)
    logging.info(df.head())
    df.reset_index(inplace=True)
    logging.info(df.columns)
    logging.info(df.head())
    df = df.astype({'date':str})
    df.to_sql(sql_table_name, engine, if_exists='replace') 

def main():
    switch_update_data = False
    output_foldername = './output/'
    os.makedirs(output_foldername, exist_ok=True)
    db_filename = output_foldername + "data.db"

    engine, session = connect_to_db(db_filename)

    with session:
        if switch_update_data is True:

            Base.metadata.drop_all(bind=engine, tables=[Wages_Cpi.__table__],checkfirst=True)
            Base.metadata.create_all(bind=engine, tables=[Wages_Cpi.__table__])
            
            startyear = 2001
            today = datetime.date.today()
            endyear = today.year
            base_line = 174.0 # cpi in 2000Q4
            """
            download_data_to_sql('CIU2020000000000A', startyear,endyear, bls_api_key, Wages.__tablename__, engine)
            print("Download the data for wages: done")
            download_data_to_sql('CUUR0000SA0', startyear,endyear, bls_api_key, Cpi.__tablename__, engine)
            print("Download the data for cpi: done")

            # copy wages data to the column
            query_result = session.query(Wages).all()
            table = Wages_Cpi.__table__
            for item in query_result:
                insert_data = dict(
                    index = item.index,
                    quarter = item.date,
                    _quarter = str_replace(item.date),
                    wages_increase_percent = item.CIU2020000000000A
                )
                insert_stmt = insert(table).values(insert_data)
                do_update_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['index'])
                session.execute(do_update_stmt)
            session.commit()
            
            # calculate the wages
            query_result = session.query(Wages_Cpi).all()
            previous_value = base_line
            for item in query_result:
                wages = previous_value * item.wages_increase_percent/400 + previous_value
                item.wages = wages
                previous_value = wages
            session.commit()

            # copy cpi data to the column
            query_result = session.query(Wages_Cpi).all()
            for item in query_result:
                cpi_query_result = session.query(Cpi).filter(Cpi.date == item._quarter).one()
                item.cpi = cpi_query_result.CUUR0000SA0
            session.commit()"""

    table_name = Wages_Cpi.__tablename__
    result = pd.read_sql(table_name, con = engine.connect(), columns = ['quarter', 'cpi','wages']) 
    correlation = result['wages'].corr(result['cpi'])
    correlation = round(correlation, 4)
    print(f"Correlation between wages and cpi is {correlation}")
    ax = result.set_index('quarter').plot(kind='line', y = ['cpi','wages'], figsize=(20,7), grid=True)
    ax.set_title("Wages and salaries vs CPI")
    ax.set_xlabel('Quater')
    ax.grid(linestyle = '--')
    ax.annotate('Result is based on data source: https://data.bls.gov/cgi-bin/surveymost?bls', (0,0), (410,-40), fontsize=8, 
             xycoords='axes fraction', textcoords='offset points', va='top')
    
    plt.savefig(output_foldername + "wages_vs_cpi.png")
    #plt.show()
    
    
if __name__ == '__main__':
    main()
import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging


# logging configuration
from config.etl_configuration import *

logging.basicConfig(
    filename="logs/etl_process.log",
    filemode='a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)



# database connection
oracle_conn = create_engine(f"oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}")
mysql_conn = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")

'''
def test_data_extraction_from_sales_data_to_stage(self):
    df_expected = pd.read_csv("test_data/sales_data.csv")
    df_actual = pd.read_sql("select * from stag_sales", mysql_conn)
    assert df_expected.equals(df_actual), "Sales data extraction did not happend correctly"
'''

def verify_expected_result_as_file_to_actual_result_as_database_table(file_path,file_type,db_conn,table_name,test_case_name):
    try:
        if file_type == "csv":
            df_expected = pd.read_csv(file_path)
        elif file_type == "json":
            df_expected = pd.read_json(file_path)
        elif file_type == "xml":
            df_expected = pd.read_xml(file_path,xpath=".//item")
        else:
            raise ValueError(f"unsupported file format passed{file_path}")
        logger.info(f"Expected data in the file is : {df_expected}")

        query_actual = f"select * from {table_name}"
        df_actual = pd.read_sql(query_actual,mysql_conn)
        logger.info(f"Actual data is : {df_actual}")

        assert df_actual.equals(df_expected),f"{test_case_name} failed as actual and expected data did not match"
    except Exception as e:
        logger.error(f"error while performing test..")







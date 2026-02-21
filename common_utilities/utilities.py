import pandas as pd
import pytest
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
'''

def verify_expected_result_as_file_to_actual_result_as_database_table(test_case_name,file_path,file_type,query_actual,db_actual):
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

        df_actual = pd.read_sql(query_actual,db_actual)
        logger.info(f"Actual data is : {df_actual}")

        # Expected minus actual data ( extra rows in expected data set will be captured)
        df_extra_in_expected = df_expected[~df_expected.apply(tuple, axis=1).isin(df_actual.apply(tuple, axis=1))]
        df_extra_in_expected.to_csv(f"differences/extra_rows_in_expetced_{test_case_name}.csv",index=False)

        # Actual minus expected data ( extra rows in actual data set will be captured)
        df_extra_in_actual = df_actual[~df_actual.apply(tuple, axis=1).isin(df_expected.apply(tuple, axis=1))]
        df_extra_in_actual.to_csv(f"differences/extra_rows_in_actual_{test_case_name}.csv", index=False)

        assert df_actual.equals(df_expected),f"{test_case_name} failed as actual and expected data did not match"
    except Exception as e:
        logger.error(f"{test_case_name} has error while performing test.Please check the diffrence in data")
        pytest.fail()


def verify_expected_result_as_database_to_actual_result_as_database_table(test_case_name, query_expected, db_expected,
                                                                      query_actual, db_actual):
    try:
        df_expected = pd.read_sql(query_expected, db_expected).astype(str)
        logger.info(f"Expected data in the file is : {df_expected}")
        df_actual = pd.read_sql(query_actual, db_actual).astype(str)
        logger.info(f"Actual data is : {df_actual}")

        # Expected minus actual data ( extra rows in expected data set will be captured)
        df_extra_in_expected = df_expected[~df_expected.apply(tuple, axis=1).isin(df_actual.apply(tuple, axis=1))]
        df_extra_in_expected.to_csv(f"differences/extra_rows_in_expetced_{test_case_name}.csv", index=False)

        # Actual minus expected data ( extra rows in actual data set will be captured)
        df_extra_in_actual = df_actual[~df_actual.apply(tuple, axis=1).isin(df_expected.apply(tuple, axis=1))]
        df_extra_in_actual.to_csv(f"differences/extra_rows_in_actual_{test_case_name}.csv", index=False)

        assert df_actual.equals(df_expected), f"{test_case_name} failed as actual and expected data did not match"
    except Exception as e:
        logger.error(f"{test_case_name} has error while performing test.Please check the diffrence in data")
        pytest.fail()



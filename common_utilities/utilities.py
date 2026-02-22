import os

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

    # Utiliies for for table existence check

def database_tables_exists(db_conn,expected_table_list,db_name):
        query = f"""select TABLE_NAME FROM information_schema.tables where 
                table_schema ='{db_name}'"""

        df = pd.read_sql(query,db_conn)
        actual_table_list = df['TABLE_NAME'].tolist()
        logger.info(f"the actual tables are :{actual_table_list}")
        missing_table_list =[]
        for tbl in expected_table_list:
            if tbl not in actual_table_list:
                missing_table_list.append(tbl)
        return missing_table_list

# File existence check
def check_for_file_existence(file_path):
    try:
        if os.path.isfile(file_path):
            return True
        else:
            return False
    except Exception as e:
        logger.error(f" file {file_path} does not exist and the error is {e}")


# File size check
def check_for_file_size(file_path):
    try:
        if os.path.getsize(file_path)!=0:
            return True
        else:
            return False
    except Exception as e:
        logger.error(f" file {file_path} is a zero byte file {e}..")






      # data quality checks related utility fucntions

def check_for_duplicates_for_specific_column_in_file(file_path,file_type,column_name):
    try:
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
            df = pd.read_json(file_path)
        elif file_type == "xml":
            df = pd.read_xml(file_path,xpath=".//item")
        else:
            raise ValueError(f"unsupported file format passed{file_path}")
        logger.info(f"The data in the file is : {df}")

        if df[column_name].diplicated().any() == True:
            return False
        else:
            return True
    except Exception as e:
        logger.error(f" error occured while pefroming duplicate checks..")


def check_for_duplicates_across_the_file(file_path, file_type):
    try:
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
            df = pd.read_json(file_path)
        elif file_type == "xml":
            df = pd.read_xml(file_path, xpath=".//item")
        else:
            raise ValueError(f"unsupported file format passed{file_path}")
        logger.info(f"The data in the file is : {df}")

        if df.duplicated().any() == True:
            return False
        else:
            return True
    except Exception as e:
        logger.error(f" error occured while pefroming duplicate checks..")


def check_for_duplicates_across_the_table(db_conn, table_name):
    pass


def check_for_duplicates_for_a_specific_column__the_table(db_conn, table_name,column_name):
    pass

# Null value ( empty ) checks in the file

def check_for_null_values_for_specific_column_in_file(file_path,file_type,column_name):
    try:
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
            df = pd.read_json(file_path)
        elif file_type == "xml":
            df = pd.read_xml(file_path,xpath=".//item")
        else:
            raise ValueError(f"unsupported file format passed{file_path}")
        logger.info(f"The data in the file is : {df}")

        if df[column_name].isnull().values.any() == True:
            return False
        else:
            return True
    except Exception as e:
        logger.error(f" error occured while pefroming duplicate checks..")



def check_for_null_values_in_file(file_path, file_type):
    try:
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
            df = pd.read_json(file_path)
        elif file_type == "xml":
            df = pd.read_xml(file_path, xpath=".//item")
        else:
            raise ValueError(f"unsupported file format passed{file_path}")
        logger.info(f"The data in the file is : {df}")

        if df.isnull().values.any() == True:
            return False
        else:
            return True
    except Exception as e:
        logger.error(f" error occured while pefroming duplicate checks..")


# referential integrity ( foreign key checks )
def check_referntial_integrity(source_db_conn,target_db_conn,foreign_query,primary_query,key_column,csv_path):
        try:
            foreign_df = pd.read_sql(foreign_query,source_db_conn)
            primary_df = pd.read_sql(primary_query, target_db_conn)
            df_not_matched = foreign_df[~foreign_df['product_id'].isin(primary_df['product_id'])]
            df_not_matched.to_csv(csv_path,index=False)
            return df_not_matched
        except Exception as e:
            logger.error(f" error occured while pefroming refreetial integrity checks..")



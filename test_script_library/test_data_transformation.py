import inspect

import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging
import pytest


# logging configuration
from common_utilities.utilities import verify_expected_result_as_file_to_actual_result_as_database_table, \
    verify_expected_result_as_database_to_actual_result_as_database_table
from config.etl_configuration import *

logging.basicConfig(
    filename="logs/etl_process.log",
    filemode='a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)

class TestDataTransformation:

    def test_data_transformation_filter_sales(self,connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            expected_query = """select * from stag_sales where sale_date>='2025-09-10'"""
            actual_query = """select * from filtered_sales"""
            verify_expected_result_as_database_to_actual_result_as_database_table(test_case_name,expected_query,connect_to_mysql_database,actual_query,connect_to_mysql_database)
        except Exception as e:
            logger.error(f"error while sales data extraction checks..")


    def test_data_transformation_Router_High_sales(self,connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            expected_query = """select * from filtered_sales where region='High'"""
            actual_query = """select * from high_sales"""
            verify_expected_result_as_database_to_actual_result_as_database_table(test_case_name,expected_query,connect_to_mysql_database,actual_query,connect_to_mysql_database)
        except Exception as e:
            logger.error(f"error while Router sales data extraction checks..")


    def test_data_transformation_Router_Low_sales(self,connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            expected_query = """select * from filtered_sales where region='Low'"""
            actual_query = """select * from low_sales"""
            verify_expected_result_as_database_to_actual_result_as_database_table(test_case_name,expected_query,connect_to_mysql_database,actual_query,connect_to_mysql_database)
        except Exception as e:
            logger.error(f"error while sales data Router transformation checks..")


  # Assignment to be completed ....
    def test_data_transformation_Aggregator_sales(self, connect_to_mysql_database):
        pass

    def test_data_transformation_Joiner(self, connect_to_mysql_database):
        pass

    def test_data_transformation_Aggretor_inventory_(self, connect_to_mysql_database):
        pass
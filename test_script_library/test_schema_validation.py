import inspect
import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging
import pytest


# logging configuration
from common_utilities.utilities import *
from config.etl_configuration import *

logging.basicConfig(
    filename="logs/etl_process.log",
    filemode='a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)

class TestSchemaValidation:

    def test_mysql_table_list_exists(self,connect_to_mysql_database):
        expected_table_list = ['fact_inventory','fact_saless','monthly_sales_summary','inventory_levels_by_store']
        try:
            missing_table_list = database_tables_exists(connect_to_mysql_database, expected_table_list, 'edw_retail_reporting')
            assert len(missing_table_list) == 0,f"missing table list is {missing_table_list} - Please check"
        except Exception as e:
            logger.error(f"error while table existence check checks..")
            pytest.fail("error while table existence check checks..")

    # Create a utility function to perform below checks using SoftAssertion
    def test_data_types_of_columns_in_the_fact_inventory(self, connect_to_mysql_database,table_name):
        pass

    def test_data_types_of_columns_in_the_fact_sales(self, connect_to_mysql_database,table_name):
        pass

    def test_data_types_of_columns_in_the_monthly_sales_summary(self, connect_to_mysql_database,table_name):
        pass

    def test_data_types_of_columns__in_the_inventory_levels_by_store(self, connect_to_mysql_database,table_name):
        pass


    # Create a utility function to perform below checks
    def test_column_name_in_the_fact_sales(self, connect_to_mysql_database,table_name):
        pass

    def test_column_name_in_the_fact_inventory(self, connect_to_mysql_database,table_name):
        pass

    def test_column_name_in_the_monthly_sales_summary(self, connect_to_mysql_database,table_name):
        pass

    def test_column_name_in_the_inventory_levels_by_store(self, connect_to_mysql_database,table_name):
        pass

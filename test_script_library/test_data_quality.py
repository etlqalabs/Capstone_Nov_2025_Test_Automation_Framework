import inspect

import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging
import pytest


# logging configuration
from common_utilities.utilities import verify_expected_result_as_file_to_actual_result_as_database_table, \
    verify_expected_result_as_database_to_actual_result_as_database_table, check_for_duplicates_across_the_file, \
    check_for_duplicates_for_specific_column_in_file, check_for_file_existence, check_for_file_size, \
    check_for_null_values_for_specific_column_in_file, check_for_null_values_in_file, check_referntial_integrity
from config.etl_configuration import *

logging.basicConfig(
    filename="logs/etl_process.log",
    filemode='a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)

class TestDataQuality:

    '''
    def test_data_quality_duplicate_check_sales_data_csv_file(self):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            duplicate_status = check_for_duplicates_across_the_file("test_data/sales_data.csv","csv")
            assert duplicate_status == True,"There are duplicates in sales_data file"
        except Exception as e:
            logger.error(f"error while duplicate check..")
            pytest.fail(f"error while duplicate check..")


    def test_data_quality_duplicate_check_product_data_csv_file(self):
        pass

    def test_data_quality_duplicate_check_supplier_data_json_file(self):
        pass

    def test_data_quality_duplicate_check_inventory_data_xml_file(self):
        pass



    # use soft assertion to perform the checks on every columns
    def test_data_quality_duplicate_check_product_id_in_sales_data_csv_file(self):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            duplicate_status_product_id = check_for_duplicates_for_specific_column_in_file("test_data/sales_data.csv","csv","product_id")
            assert duplicate_status_product_id == True,"There are duplicates in product_id column in sales_data file"
            # Add assertion for all the columns

        except Exception as e:
            logger.error(f"error while duplicate check..")
            pytest.fail(f"error while duplicate check..")

    def test_data_quality_duplicate_check_product_id_in_product_data_csv_file(self):
        pass

    def test_data_quality_duplicate_check_supplier_id_in_supplier_data_json_file(self):
        pass

    def test_data_quality_duplicate_check_product_id_in_inventory_data_xml_file(self):
        pass

    # Duplicate checks o table and coly=umns as well
    def test_data_quality_duplicate_check_in_fact_sales(self,connect_to_mysql_database):
         pass

    def test_data_quality_duplicate_check_product_id_in_fact_sales(self, connect_to_mysql_database):
        pass

    # Add remaining test case for rest of the tables and on different columns



    # File existence check
    def test_data_quality_file_existence_of_sales_data_csv_file(self):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            file_existence_status = check_for_file_existence("test_data/sales_data.csv")
            assert file_existence_status == True,f"sales_data csv file doen not exist in the location"
        except Exception as e:
            logger.error(f"error while reading the file..")
            pytest.fail(f"eerror while reading the file..")

    def test_data_quality_file_existence_of_product_data_csv_file(self):
        pass

    def test_data_quality_file_existence_of_supplier_data_json_file(self):
        pass

    def test_data_quality_file_existence_of_inventory_data_xml_file(self):
        pass


# File size check check
    def test_data_quality_file_size_of_sales_data_csv_file(self):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            file_existence_status = check_for_file_size("test_data/sales_data.csv")
            assert file_existence_status == True,f"sales_data csv file doen not have any data in the location"
        except Exception as e:
            logger.error(f"error while reading the file..")
            pytest.fail(f"error while reading the file..")

    def test_data_quality_file_size_of_product_data_csv_file(self):
        pass

    def test_data_quality_file_size_of_supplier_data_json_file(self):
        pass

    def test_data_quality_file_size_of_inventory_data_xml_file(self):
        pass

    # NULL value checks on a column in the file
    def test_data_quality_null_chec_value_product_id_in_sales_data_csv_file(self):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            null_status_product_id = check_for_null_values_for_specific_column_in_file("test_data/sales_data.csv","csv","product_id")
            assert null_status_product_id == True,"There are null values  in product_id column in sales_data file"
        except Exception as e:
            logger.error(f"error while checking null values..")
            pytest.fail(f"error while checking null values..")

      # Implement the checks for all other files and columns

    # NULL value checks in the file
    def test_data_quality_null_check_value_in_sales_data_csv_file(self):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            null_values__status = check_for_null_values_in_file("test_data/sales_data.csv",
                                                                                            "csv")
            assert null_values__status == True, "There are null values  in product_id column in sales_data file"
        except Exception as e:
            logger.error(f"error while checking null values..")
            pytest.fail(f"error while checking null values..")

         # Implement the checks for all other files and columns

        # Implement the checks for tables as well

'''

    #referential integerity check
    def test_data_quality_refererntial_check_fact_sales_and_stag_product(self,connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            foreign_query = """select * from fact_sales"""
            primary_query= """select * from stag_product"""
            df_not_matched = check_referntial_integrity(
                source_db_conn = connect_to_mysql_database,
                target_db_conn = connect_to_mysql_database,
                foreign_query = foreign_query,
                primary_query = primary_query,
                key_column = "product_id",
                csv_path = "differences/foregin_key_not_mathcing.csv")

            assert df_not_matched.empty == True,"There are foreign keys values  in product_id column in foregin key table"
        except Exception as e:
            logger.error(f"error while regerential integery check.")
            pytest.fail(f"error while regerential integery check.")



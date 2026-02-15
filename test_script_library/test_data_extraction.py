import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging
import pytest


# logging configuration
from common_utilities.utilities import verify_expected_result_as_file_to_actual_result_as_database_table
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

class TestDataExtraction:
    def test_data_extraction_from_sales_data_to_stage(self):
        try:
            test_case_name = "sales_data extraction"
            verify_expected_result_as_file_to_actual_result_as_database_table("test_data/sales_data.csv","csv",mysql_conn,"stag_sales",test_case_name)
        except Exception as e:
            logger.error(f"error while sales data extraction checks..")


    def test_data_extraction_from_supplier_data_to_stage(self):
        try:
            test_case_name = "product_data extraction"
            verify_expected_result_as_file_to_actual_result_as_database_table("test_data/supplier_data.json","json",mysql_conn,"stag_supplier",test_case_name)
        except Exception as e:
            logger.error(f"error while supplier data extraction checks..")

    @pytest.mark.skip
    def test_data_extraction_from_product_data_to_stage(self):
        pass

import unittest
import glob
import os
from pyspark.sql import SparkSession


class TestMain(unittest.TestCase):
    spark = None

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("Unit-tests").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_should_write_json_output_given_valid_input_data(self):
        # GIVEN
        main_file = "main.py"

        # WHEN
        os.system("spark-submit " + main_file)

        # THEN
        list_of_files = glob.glob('output/json/*.json')
        latest_file = max(list_of_files, key=os.path.getctime)
        self.assertRegex(latest_file, r'part-0000.*\.json')

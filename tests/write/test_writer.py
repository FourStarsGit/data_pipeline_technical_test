import shutil
import unittest
import glob
import os
from pyspark.sql import SparkSession
from src.write.writer import Writer
from pyspark.sql.types import Row


class TestWriter(unittest.TestCase):
    spark = None

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("Unit-tests").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        shutil.rmtree('output_test')

    def test_should_write_valid_df_to_json_output(self):
        # GIVEN
        denormalized_df = self.spark.createDataFrame([Row(drug='DIPHENHYDRAMINE', atccode='A04AD',
                                                          pubmed_dates=['01/01/2019'], trial_dates=['01/01/2020'],
                                                          journals_and_dates=[Row(
                                                              journal='Journal of emergency nursing',
                                                              date='01/01/2019'),
                                                              Row(journal='Journal of emergency nursing',
                                                                  date='01/01/2020')])])
        writer = Writer(denormalized_df)

        # WHEN
        writer.to_json("output_test/json")

        # THEN
        list_of_files = glob.glob('output_test/json/*.json')
        latest_file = max(list_of_files, key=os.path.getctime)
        self.assertRegex(latest_file, r'part-0000.*\.json')

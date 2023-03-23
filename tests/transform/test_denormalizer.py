import unittest
from pyspark.sql import SparkSession
from src.transform.denormalizer import Denormalizer
from pyspark.sql.types import Row


class TestDenormalizer(unittest.TestCase):
    spark = None

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("Unit-tests").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_should_denormalize_three_df(self):
        # GIVEN
        drugs = self.spark.createDataFrame([Row(atccode='A04AD', drug='DIPHENHYDRAMINE')])
        pubmed = self.spark.createDataFrame([Row(id='1',
                                                 title='A 44-year-old man with erythema of the face diphenhydramine, '
                                                       'neck, and ''chest, weakness, and palpitations',
                                                 date='01/01/2019',
                                                 journal='Journal of emergency nursing')])
        clinical_trials = self.spark.createDataFrame([Row(id='NCT01967433',
                                                          scientific_title='Use of Diphenhydramine as an Adjunctive '
                                                                           'Sedative for Colonoscopy in Patients '
                                                                           'Chronically on Opioids',
                                                          date='01/01/2020',
                                                          journal='Journal of emergency nursing')])
        denormalizer = Denormalizer(drugs, pubmed, clinical_trials)

        # WHEN
        denormalized_df = denormalizer.to_linked_graph_df()

        # THEN
        assert denormalized_df.first() == Row(drug='DIPHENHYDRAMINE', atccode='A04AD', pubmed_dates=['01/01/2019'],
                                              trial_dates=['01/01/2020'],
                                              journals_and_dates=[Row(journal='Journal of emergency nursing',
                                                                      date='01/01/2019'),
                                                                  Row(journal='Journal of emergency nursing',
                                                                      date='01/01/2020')])


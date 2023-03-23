from src.extract.extractor import Extractor
import unittest
from pyspark.sql.types import Row
from pyspark.sql import SparkSession


class TestExtractor(unittest.TestCase):
    spark = None

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("Unit-tests").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_should_return_valid_df_given_csv_file_with_header(self):
        # GIVEN
        extractor = Extractor(self.spark)
        path = "resources/data/drugs.csv"

        # WHEN
        result = extractor.from_csv(path).first()

        # THEN
        assert result == Row(atccode='A04AD', drug='DIPHENHYDRAMINE')

    def test_should_return_valid_df_given_json_file(self):
        # GIVEN
        extractor = Extractor(self.spark)
        path = "resources/data/pubmed.json"

        # WHEN
        result = extractor.from_json(path).first()

        # THEN
        assert result == Row(date='01/01/2020', id='9',
                             journal='Journal of photochemistry and photobiology. B, Biology',
                             title='Gold nanoparticles synthesized from Euphorbia fischeriana root by green route '
                                   'method alleviates the isoprenaline hydrochloride induced myocardial infarction '
                                   'in rats.')

    def test_should_return_three_df_given_all_four_data_paths(self):
        # GIVEN
        extractor = Extractor(self.spark)
        drugs_file = "resources/data/drugs.csv"
        pubmed_csv = "resources/data/pubmed.csv"
        pubmed_json = "resources/data/pubmed.json"
        trials_file = "resources/data/clinical_trials.csv"

        # WHEN
        drugs_df, pubmed_df, trials_df = extractor.load_all(drugs_file, pubmed_csv, pubmed_json, trials_file)

        # THEN
        assert drugs_df.first() == Row(atccode='A04AD', drug='DIPHENHYDRAMINE')
        assert pubmed_df.first() == Row(id='1',
                                        title='A 44-year-old man with erythema of the face diphenhydramine, neck, and '
                                              'chest, weakness, and palpitations', date='01/01/2019',
                                        journal='Journal of emergency nursing')
        assert trials_df.first() == Row(id='NCT01967433',
                                        scientific_title='Use of Diphenhydramine as an Adjunctive Sedative for '
                                                         'Colonoscopy in Patients Chronically on Opioids',
                                        date='01/01/2020', journal='Journal of emergency nursing')

from pyspark.sql.functions import udf, regexp_replace
import re
from datetime import datetime


class Extractor:

    def __init__(self, spark):
        self.spark = spark

    journal_col = "journal"
    bad_characters = "\\\\xc3\\\\x28"

    def load_all(self, drugs_file, pubmed_csv, pubmed_json, trials_file):
        # UDF used to standardize dates with unique format : %d/%m/%Y
        @udf
        def to_standard_date_format(x):
            if re.match(r"\d{2}/\d{2}/\d{4}", x):
                return x
            elif re.match(r"\d{4}-\d{2}-\d{2}", x):
                return datetime.strptime(x, "%Y-%m-%d").strftime("%d/%m/%Y")
            else:
                return datetime.strptime(x, "%d %B %Y").strftime("%d/%m/%Y")

        # Loading all files to DataFrames and apply some cleaning
        drugs = self.from_csv(drugs_file)
        pubmed_from_csv = self.from_csv(pubmed_csv).withColumn("date", to_standard_date_format("date"))
        pubmed_from_json = self.from_json(pubmed_json).withColumn("date", to_standard_date_format("date"))
        pubmed = pubmed_from_csv.union(pubmed_from_json.select(pubmed_from_csv.columns))
        trials = self.from_csv(trials_file). \
            withColumn(self.journal_col, regexp_replace(self.journal_col, self.bad_characters, "")). \
            withColumn("date", to_standard_date_format("date"))
        return drugs, pubmed, trials

    def from_csv(self, path):
        return self.spark.read.option("header", True).csv(path)

    def from_json(self, path):
        return self.spark.read.json(path)

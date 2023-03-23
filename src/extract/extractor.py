from pyspark.sql.functions import udf, regexp_replace


class Extractor:

    def __init__(self, spark):
        self.spark = spark

    journal_col = "journal"
    bad_characters = "\\\\xc3\\\\x28"

    def load_all(self, drugs_file, pubmed_csv, pubmed_json, trials_file):
        @udf
        def ascii_udf(x):
            return x.encode('ascii', 'ignore').decode('ascii') if x else None

        drugs = self.from_csv(drugs_file)
        pubmed_from_csv = self.from_csv(pubmed_csv)
        pubmed_from_json = self.from_json(pubmed_json)
        pubmed = pubmed_from_csv.union(pubmed_from_json.select(pubmed_from_csv.columns))
        trials = self.from_csv(trials_file).\
            withColumn(self.journal_col, regexp_replace(self.journal_col, self.bad_characters, ""))
        return drugs, pubmed, trials

    def from_csv(self, path):
        return self.spark.read.option("header", True).csv(path)

    def from_json(self, path):
        return self.spark.read.json(path)

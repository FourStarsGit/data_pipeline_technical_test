class Extractor:

    def __init__(self, spark):
        self.spark = spark

    def load_all(self, drugs_file, pubmed_csv, pubmed_json, trials_file):
        drugs = self.from_csv(drugs_file)
        pubmed_from_csv = self.from_csv(pubmed_csv)
        pubmed_from_json = self.from_json(pubmed_json)
        pubmed = pubmed_from_csv.union(pubmed_from_json.select(pubmed_from_csv.columns))
        trials = self.from_csv(trials_file)
        return drugs, pubmed, trials

    def from_csv(self, path):
        return self.spark.read.option("header", True).csv(path)

    def from_json(self, path):
        return self.spark.read.json(path)

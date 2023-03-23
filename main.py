from pyspark.sql import SparkSession
from src.extract.extractor import Extractor
from src.transform.denormalizer import Denormalizer
from src.write.writer import Writer


# TODO Convertir les fichiers JSON d'entrée en JSON records
# TODO le mode TRACE pour les logs

if __name__ == '__main__':
    spark = SparkSession.builder.appName("ServierTechnicalTest").getOrCreate()

    # Extract data into dataframe
    extractor = Extractor(spark)

    drugs, pubmed, clinical_trials = \
        extractor.load_all("resources/data/drugs.csv", "resources/data/pubmed.csv",
                           "resources/data/pubmed.json", "resources/data/clinical_trials.csv")

    drugs.show()
    pubmed.show()
    clinical_trials.show(truncate=False)

    # Denormalize data
    denormalizer = Denormalizer(drugs, pubmed, clinical_trials)
    denormalized_df = denormalizer.to_linked_graph_df()

    denormalized_df.show(truncate=False)

    # Write to output
    writer = Writer(denormalized_df)
    writer.to_json("output/json")

from pyspark.sql import SparkSession
import sys
from src.extract.extractor import Extractor
from src.transform.denormalizer import Denormalizer
from src.write.writer import Writer


def is_trace_enabled():
    log_manager = spark._jvm.org.apache.log4j.LogManager
    logger = log_manager.getRootLogger()
    return logger.isEnabledFor(spark._jvm.org.apache.log4j.Level.TRACE)


if __name__ == '__main__':
    output_path = "output/json/" if len(sys.argv) <= 1 else sys.argv[1]
    spark = SparkSession.builder.appName("ServierTechnicalTest").getOrCreate()

    # Extract data into dataframe
    extractor = Extractor(spark)

    drugs, pubmed, clinical_trials = \
        extractor.load_all("resources/data/drugs.csv", "resources/data/pubmed.csv",
                           "resources/data/pubmed.json", "resources/data/clinical_trials.csv")

    if is_trace_enabled():
        drugs.show()
        pubmed.show()
        clinical_trials.show(truncate=False)

    # Denormalize data
    denormalizer = Denormalizer(drugs, pubmed, clinical_trials)
    denormalized_df = denormalizer.to_linked_graph_df(is_trace_enabled())

    if is_trace_enabled():
        denormalized_df.show(truncate=False)

    # Write to output
    writer = Writer(denormalized_df)
    writer.to_json(output_path)

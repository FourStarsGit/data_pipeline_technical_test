from pyspark.sql import SparkSession
import pyspark.sql.functions as f


# TODO output path en paramètre
if __name__ == '__main__':
    spark = SparkSession.builder.appName("ServierTechnicalTest").getOrCreate()

    journals_col = "journals"
    count_col = "count"

    df = spark.read.json("output/json/")
    result = df.select(f.explode(f.array_distinct(df.journals_and_dates.journal)).alias(journals_col)). \
        groupBy(journals_col).agg(f.count(journals_col).alias(count_col))

    max_drugs = result.select(count_col).orderBy(f.desc(count_col)).first().__getitem__(count_col)
    journals = result.filter(f.col(count_col) == max_drugs).select(journals_col).collect()

    selected_journals = ", ".join(map(lambda x: x.__getitem__(journals_col), journals))

    print("The journal(s) which contain(s) the most drugs is/are: " + selected_journals +
          ". The maximum drugs mentioned is " + str(max_drugs) + ".")





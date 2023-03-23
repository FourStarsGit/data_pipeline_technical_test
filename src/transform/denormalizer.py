import pyspark.sql.functions as f


class Denormalizer:

    pubmed_dates = "pubmed_dates"
    trial_dates = "trial_dates"
    journal_pubmed_dates = "journal_pubmed_dates"
    journal_trial_dates = "journal_trial_dates"
    journals_and_dates = "journals_and_dates"
    outer_mode = "full_outer"
    drug = "drug"

    def __init__(self, drugs, pubmed, trials):
        self.drugs = drugs
        self.pubmed = pubmed
        self.trials = trials

    def to_linked_graph_df(self, is_trace_enabled):

        # Denormalize and merge drugs and pubmed data
        drugs_and_pubmed = \
            self.drugs.join(self.pubmed, f.lower(self.pubmed.title).contains(f.lower(self.drugs.drug))). \
            groupBy(self.drugs.drug, self.drugs.atccode). \
            agg(f.collect_list(self.pubmed.date).alias(self.pubmed_dates),
                f.collect_list(f.struct(self.pubmed.journal, self.pubmed.date)).alias(self.journal_pubmed_dates))

        if is_trace_enabled:
            drugs_and_pubmed.show(truncate=False)

        # Denormalize and merge drugs and clinical trials data
        drugs_and_trials = \
            self.drugs.join(self.trials,
                            f.lower(self.trials.scientific_title).contains(f.lower(self.drugs.drug))).\
            groupBy(self.drugs.drug). \
            agg(f.collect_list(self.trials.date).alias(self.trial_dates),
                f.collect_list(f.struct(self.trials.journal, self.trials.date)).alias(self.journal_trial_dates))

        if is_trace_enabled:
            drugs_and_trials.show(truncate=False)

        # Re-conciliate both dataframes
        return drugs_and_pubmed.join(drugs_and_trials, [self.drug], how=self.outer_mode). \
            withColumn(self.journal_pubmed_dates, self.clean_null(drugs_and_pubmed.journal_pubmed_dates)). \
            withColumn(self.journal_trial_dates, self.clean_null(drugs_and_trials.journal_trial_dates)). \
            withColumn(self.trial_dates, self.clean_null(drugs_and_trials.trial_dates)). \
            withColumn(self.pubmed_dates, self.clean_null(drugs_and_pubmed.pubmed_dates)). \
            withColumn(self.journals_and_dates,
                       f.flatten(f.array(self.journal_pubmed_dates, self.journal_trial_dates))). \
            drop(self.journal_pubmed_dates, self.journal_trial_dates)

    @staticmethod
    def clean_null(col):
        return f.when(col.isNull(), f.array()).otherwise(col)

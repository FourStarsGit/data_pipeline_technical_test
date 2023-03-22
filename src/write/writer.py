class Writer:

    def __init__(self, df):
        self.df = df

    def to_json(self, path):
        self.df.write.mode("overwrite").json(path)
        print("Data have been successfully save as JSON records")

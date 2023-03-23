# Class used for load step
class Writer:

    def __init__(self, df):
        self.df = df

    # Write DataFrame as JSON to selected path
    def to_json(self, path):
        self.df.write.mode("overwrite").json(path)
        print("Data have been successfully save as JSON records")

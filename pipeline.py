import pandas as pd
import glob


# post-processing class
class Pipeline:
    def __init__(self, folder_name):
        df_list = []
        for filename in glob.glob(folder_name + "/*.csv"):
            try:
                df1 = pd.read_csv(filename, skiprows=3, error_bad_lines=False, warn_bad_lines=False)
                df2 = []
                df3 = []
                for i, row in enumerate(df1.isna()):
                    if row:
                        print(i)
                #df2 = pd.read_csv(filename, skiprows=(4 + len(df1)), error_bad_lines=False, warn_bad_lines=False)
                #df3 = pd.read_csv(filename, skiprows=(5 + len(df1) + len(df2)), error_bad_lines=False, warn_bad_lines=False)
            except pd.errors.EmptyDataError:
                print(filename + " is causing problems. Check the formatting.")
            df_list.append((df1, df2, df3))
        self.df_list = df_list
        print(df_list[0][0])


def clean_data(self):
    for data in self.data_list:
        pass
    print(self.data_list[0])


if __name__ == "__main__":
    bass_pipe = Pipeline("Data")
    #bass_pipe.clean_data()

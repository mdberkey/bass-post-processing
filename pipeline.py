import numpy as np
import pandas as pd
import glob


# post-processing class
class Pipeline:
    def __init__(self, folder_name):
        df_list = []
        for filename in glob.glob(folder_name + '/*.csv'):
            try:
                df1 = pd.read_csv(filename, names=['key', 'value'], skiprows=3, nrows=15)
                df2 = pd.read_csv(filename, skiprows=18)

                print(df2.index[df2['Observer'] == 'BehavCode'])
                #df_list = np.split(df2, df2[df2.isnull().all(1)].index)
                #print(df2)
                #for df in df_list:
                #    print(df, '\n\n\n\n\n\n\n')
                    #blank_rows = df2.iloc[:, 1].isna()
                #sep_row = [i for i, x in enumerate(blank_rows) if x]
                #blank_rows = initial_rows.iloc[:, 1].isna()
                #print(blank_rows)
                #if not blank_rows[13]:
                #    print(filename)
                #df1 = pd.read_csv(filename, skiprows=3, error_bad_lines=False, warn_bad_lines=False)
                df2 = []
                df3 = []
                #for i, row in enumerate(df1.isna()):
                #    if row:
                 #       print(i)
                #df2 = pd.read_csv(filename, skiprows=(4 + len(df1)), error_bad_lines=False, warn_bad_lines=False)
                #df3 = pd.read_csv(filename, skiprows=(5 + len(df1) + len(df2)), error_bad_lines=False, warn_bad_lines=False)
            except pd.errors.EmptyDataError:
                print(filename + " is causing problems. Check the formatting.")
            #df_list.append((df1, df2, df3))
        self.df_list = df_list
        #print(df_list[0][0])


def clean_data(self):
    for data in self.data_list:
        pass
    print(self.data_list[0])


if __name__ == "__main__":
    bass_pipe = Pipeline("Data")
    #bass_pipe.clean_data()

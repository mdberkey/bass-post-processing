import numpy as np
import pandas as pd
import glob


# post-processing class
class Pipeline:
    def __init__(self, folder_name):
        self.df_list = []
        for filename in glob.glob(folder_name + '/*.csv'):
            try:
                df1 = pd.read_csv(filename, names=['key', 'value'], skiprows=3, nrows=15)
                df_temp = pd.read_csv(filename, skiprows=18)

                split_index = df_temp.index[df_temp['Observer'] == 'BehavCode'][0]
                df2 = df_temp.iloc[:split_index, :]
                df2.columns = df2.columns.str.lstrip()
                df3 = df_temp.iloc[split_index + 1:, :].dropna(axis=1)
                df3_header = ['BehavCode', 'Occurrences', 'totalTime']
                df3.columns = df3_header
                self.df_list.append([df1, df2, df3])
            except pd.errors.EmptyDataError:
                print(filename + ' is causing problems. Check the formatting.')

    def calc_values(self):
        df_behav = self.df_list[1][1]
        df_calc = pd.DataFrame(
            [
            'Inactive',
            'Locomotion',
            'Pacing',
            'Jumping',
            'Selfbite',
            'Selfdirect',
            'Swing / spin / flip',
            'Headtoss',
            'Rock',
            'Salute',
            'Feargrimace',
            'Scratch',
            'Yawn',
            'Lipsmack',
            'Present',
            'Cling',
            'Mantleshake',
            'Vocal',
            'Threat / display',
            'Aggression',
            'Eat / drink',
            'Tactile / explore',
            'Social / play',
            'Groom',
            'SocGroom',
            'Sex / self / other',
            'Other'
            ]
        )
        print(df_calc[0].tolist())
        for type in df_calc[0].tolist():
            for behav in df_behav['BehavCode'].tolist():
                if type == behav:
                   pass

         #   print(behav)

        #df_calc['Uniques aligned'] = ['3', '4']
        #df_calc.columns = ['Behavior list', 'Uniques aligned', 'Uniques', 'Frequency Align', 'Duration Align']
        #print(df_calc)


    def export_excel(self):
        info_df = pd.DataFrame(['Focal Animal Recording', '(c)Michael J. Renner', ''])
        clear_df2 = self.df_list[1]
        #print(info_df)
        #self.df_list[0].to_excel('test.xlsx', startrow=3, index=False, header=False)
        #info_df.to_excel('test.xlsx', index=False, header=False)

        append_df_to_excel(filename='test.xlsx', df=info_df, startrow=0, index=False, header=False)
        append_df_to_excel(filename='test.xlsx', df=self.df_list[0], startrow=3, index=False, header=False)
        append_df_to_excel(filename='test.xlsx', df=self.df_list[1], startrow=19, index=False)


def append_df_to_excel(filename, df, sheet_name='Sheet1', startrow=None,
                       truncate_sheet=False,
                       **to_excel_kwargs):
    """
    Append a DataFrame [df] to existing Excel file [filename]
    into [sheet_name] Sheet.
    If [filename] doesn't exist, then this function will create it.

    @param filename: File path or existing ExcelWriter
                     (Example: '/path/to/file.xlsx')
    @param df: DataFrame to save to workbook
    @param sheet_name: Name of sheet which will contain DataFrame.
                       (default: 'Sheet1')
    @param startrow: upper left cell row to dump data frame.
                     Per default (startrow=None) calculate the last row
                     in the existing DF and write to the next row...
    @param truncate_sheet: truncate (remove and recreate) [sheet_name]
                           before writing DataFrame to Excel file
    @param to_excel_kwargs: arguments which will be passed to `DataFrame.to_excel()`
                            [can be a dictionary]
    @return: None
    """
    # Excel file doesn't exist - saving and exiting
    if not os.path.isfile(filename):
        df.to_excel(
            filename,
            sheet_name=sheet_name,
            startrow=startrow if startrow is not None else 0,
            **to_excel_kwargs)
        return

    # ignore [engine] parameter if it was passed
    if 'engine' in to_excel_kwargs:
        to_excel_kwargs.pop('engine')

    writer = pd.ExcelWriter(filename, engine='openpyxl', mode='a')

    # try to open an existing workbook
    writer.book = load_workbook(filename)

    # get the last row in the existing Excel sheet
    # if it was not specified explicitly
    if startrow is None and sheet_name in writer.book.sheetnames:
        startrow = writer.book[sheet_name].max_row

    # truncate sheet
    if truncate_sheet and sheet_name in writer.book.sheetnames:
        # index of [sheet_name] sheet
        idx = writer.book.sheetnames.index(sheet_name)
        # remove [sheet_name]
        writer.book.remove(writer.book.worksheets[idx])
        # create an empty sheet [sheet_name] using old index
        writer.book.create_sheet(sheet_name, idx)

    # copy existing sheets
    writer.sheets = {ws.title: ws for ws in writer.book.worksheets}

    if startrow is None:
        startrow = 0

    # write out the new sheet
    df.to_excel(writer, sheet_name, startrow=startrow, **to_excel_kwargs)

    # save the workbook
    writer.save()

if __name__ == '__main__':
    bass_pipe = Pipeline('Data')
    bass_pipe.calc_values()

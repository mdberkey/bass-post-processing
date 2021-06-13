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
                self.df_list.append([df1, df2, df3, filename[5:-4]])
            except pd.errors.EmptyDataError:
                print(filename + ' is causing problems. Check the formatting.')


    def calc_cum_data(self):
        cum_data = pd.DataFrame(columns=self.HEADERS)

        for df_group in self.df_list:

            # df1 data
            cum_row = [df_group[3]]
            # cum_data initial order
            for i in [2, 7, 3, 0, 10, 11, 12, 9, 1, 12, 12, 4, 5, 6]: #TODO: Find out differences...
                if i is None:
                    cum_row.append(None)
                    cum_row.append(None)
                else:
                    cum_row.append(df_group[0]['value'][i])

            # df2 data
            behav_dict = {
                'Inactive': [],
                'Locomotion': [],
                'Pacing': [],
                'Jumping': [],
                'Selfbite': [],
                'Selfdirect': [],
                'Swing/spin/flip': [],
                'Headtoss': [],
                'Rock': [],
                'Salute': [],
                'Feargrimace': [],
                'Scratch': [],
                'Yawn': [],
                'Lipsmack': [],
                'Present': [],
                'Cling': [],
                'Mantleshake': [],
                'Vocal': [],
                'Threat/display': [],
                'Aggression': [],
                'Eat/drink': [],
                'Tactile/explore': [],
                'Social/play': [],
                'Groom': [],
                'SocGroom': [],
                'Sex/self/other': [],
                'Other': []
            }
            unique_behavs = 0
            for behav, dur in zip(df_group[1]['BehavCode'], df_group[1]['Duration']):
                dur = dur[3:10]
                # values : frequency, total Duration, Avg. Duration, list of timestamps
                prev_values = behav_dict[behav]
                new_values = []
                if not prev_values:
                    new_values = [1, dur, dur, [dur]]
                    unique_behavs += 1
                else:
                    new_dur_list = prev_values[3]
                    new_dur_list.append(dur)
                    new_values = [
                        prev_values[0] + 1,
                        self.get_total_duration(new_dur_list),
                        self.get_avg_duration(new_dur_list),
                        new_dur_list
                    ]
                behav_dict[behav] = new_values

            most_freq = ['', 0]
            dur_tup_list = []
            NBC_most_freq = ['', 0]
            NBC_dur_tup_list = []
            for key in behav_dict:
                if behav_dict[key]:
                    freq = behav_dict[key][0]
                    if freq > most_freq[1]:
                        most_freq = [key, freq]
                    dur_tup_list.append((key, behav_dict[key][1]))

                    if key not in self.NBC_dict:
                        if freq > NBC_most_freq[1]:
                            NBC_most_freq = [key, freq]
                        NBC_dur_tup_list.append((key, behav_dict[key][1]))
            long_dur = self.get_max_duration(dur_tup_list)
            if behav_dict['Groom']:
                other_freq_dur = (behav_dict['Groom'][0], behav_dict['Groom'][1])
            else:
                other_freq_dur = (0, '00:00.0')
            NBC_long_dur = self.get_max_duration(NBC_dur_tup_list)

            if not NBC_long_dur:
                NBC_long_dur = (None, '00:00.0')    #TODO more of this

            cum_row.extend(
                [
                    unique_behavs,
                    most_freq[0],
                    most_freq[1],
                    long_dur[0],
                    long_dur[1],
                    other_freq_dur[0],
                    other_freq_dur[1],
                    NBC_most_freq[0],
                    NBC_most_freq[1],
                    NBC_long_dur[0],
                    NBC_long_dur[1]
                ]
            )
            for key in behav_dict:
                if behav_dict[key]:
                    cum_row.extend([behav_dict[key][0], behav_dict[key][1], behav_dict[key][2]])
                else:
                    cum_row.extend([0, '00:00.0', None])
            print(cum_row)


    def get_avg_duration(self, dur_list):
        """
        Calculates the average timestamp of all timestamps in a list
        :param dur_list: list of timestamps
        :return: average timestamp string
        """
        total_sec = 0
        for dur in dur_list:
            total_sec += (int(dur[:2]) * 60) + float(dur[3:])
        sec_avg = total_sec / len(dur_list)
        time_min = int(sec_avg / 60)
        time_sec = sec_avg - (time_min * 60)
        return f'{time_min:02d}:{time_sec:04.1f}'

    def get_total_duration(self, dur_list):
        """
        Calculates the total timestamp of dur_list
        :param dur_list: list of timestamps
        :return: total duration timestamp string
        """
        total_sec = 0
        for dur in dur_list:
            total_sec += (int(dur[:2]) * 60) + float(dur[3:])
        time_min = int(total_sec / 60)
        time_sec = total_sec - (time_min * 60)
        return f'{time_min:02d}:{time_sec:04.1f}'

    def get_max_duration(self, dur_tup_list):
        """
        Calculates the max timestamp of dur_list
        :param dur_list: list of timestamps
        :return: max duration timestamp string
        """
        max_dur = [0, '']
        for dur_tup in dur_tup_list:
            dur_sec = (int(dur_tup[1][:2]) * 60) + float(dur_tup[1][3:])
            if dur_sec > max_dur[0]:
                max_dur = [dur_sec, dur_tup]
        return max_dur[1]




    # Constants
    HEADERS = [
        'Sheetname',
        'ID',
        'Date',
        'Site',
        'Obs',
        'start',
        'time',
        'End',
        'time',
        'Ses',
        'Duration',
        'Ethogram',
        'Study',
        'code',
        'Tot',
        'Duration',
        'Ses',
        'Dur',
        'Obs',
        'type',
        'Housing',
        'Room / cage',
        'Unique',
        'Beh',
        'Most',
        'frequent',
        'Freq',
        'Long',
        'Dura',
        'Duration',
        'Other',
        'Freq',
        'Other',
        'Dur',
        'Hi',
        'NBC',
        'beh',
        'Freq',
        'NBC',
        'Freq',
        'Hi',
        'NBC',
        'Beh',
        'Dur',
        'NBC',
        'Dur',
        'Inactive',
        'Frequency',
        'Inactive',
        'Duration',
        'Inactive',
        'Ave',
        'duration',
        'Loco',
        'Frequency',
        'Loco',
        'Duration',
        'Loco',
        'Ave',
        'dur',
        'Pacing',
        'Frequency',
        'Pacing',
        'Duration',
        'Pacing',
        'ave',
        'dur',
        'Jumping',
        'Freq',
        'Jumping',
        'Duration',
        'Jumping',
        'ave',
        'dur',
        'Selfbite',
        'Frequency',
        'Selfbite',
        'Duration',
        'Selfbite',
        'Ave',
        'Dur',
        'Selfdirect',
        'Frequency',
        'Selfdirect',
        'Duration',
        'Selfdirect',
        'Ave',
        'Dur',
        'Swing / spin / flip',
        'Frequency',
        'Swing / spin / flip',
        'Duration',
        'Swing / spin / flip',
        'Ave',
        'Dur',
        'Headtoss',
        'Frequency',
        'Headtoss',
        'Duration',
        'Headtoss',
        'Ave',
        'Dur',
        'Rock',
        'Frequency',
        'Rock',
        'Duration',
        'Rock',
        'Ave',
        'Dur',
        'Salute',
        'Frequency',
        'Salute',
        'Duration',
        'Salute',
        'Ave',
        'Dur',
        'Feargrimace',
        'Frequency',
        'Feargrimace',
        'Duration',
        'Feargrimace',
        'Ave',
        'Dur',
        'Scratch',
        'Frequency',
        'Scratch',
        'Duration',
        'Scratch',
        'Ave',
        'Dur',
        'Yawn',
        'Frequency',
        'Yawn',
        'Duration',
        'Yawn',
        'Ave',
        'Dur',
        'Lipsmack',
        'Frequency',
        'Lipsmack',
        'Duration',
        'Lipsmack',
        'Ave',
        'Dur',
        'Present',
        'Frequency',
        'Present',
        'Duration',
        'Present',
        'Ave',
        'Dur',
        'Cling',
        'Frequency',
        'Cling',
        'Duration',
        'Cling',
        'Ave',
        'Dur',
        'Mantleshake',
        'Frequency',
        'Mantleshake',
        'Duration',
        'Mantleshake',
        'Ave',
        'Dur',
        'Vocal',
        'Frequency',
        'Vocal',
        'Duration',
        'Vocal',
        'Ave',
        'Dur',
        'Threat / display',
        'Frequency',
        'Threat / display',
        'Duration',
        'Threat / display',
        'Ave',
        'Dur',
        'Aggression',
        'Frequency',
        'Aggression',
        'Duration',
        'Aggression',
        'Ave',
        'Dur',
        'Eat / drink',
        'Frequency',
        'Eat / drink',
        'Duration',
        'Eat / drink',
        'Ave',
        'Dur',
        'Tactile / explore',
        'Frequency',
        'Tactile / explore',
        'Duration',
        'Tactile / explore',
        'Ave',
        'Dur',
        'Social / play',
        'Frequency',
        'Social / play',
        'Duration',
        'Social / play',
        'Ave',
        'Dur',
        'Groom',
        'Frequency',
        'Groom',
        'Duration',
        'Groom',
        'Ave',
        'Dur',
        'SocGroom',
        'Frequency',
        'SocGroom',
        'Duration',
        'SocGroom',
        'Ave',
        'Dur',
        'Sex / self / other',
        'Frequency',
        'Sex / self / other',
        'Duration',
        'Sex / self / other',
        'Ave',
        'Dur',
        'Other',
        'Frequency',
        'Other',
        'Duration',
        'Other',
        'Ave',
        'Dur',
        'Notes',
        'SIB',
        'Inactive',
        '90 % Pacing',
        '25 % Pacing',
        '50 % Vocal',
        '25 %'
    ]

    NBC_dict = [
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

    # df1 data

    def bruh(self):
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
        #print(df_calc[0].tolist())
        #for type in df_calc[0].tolist():
        #    for behav in df_behav['BehavCode'].tolist():
         #       if type == behav:
          #         pass

        data

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
    bass_pipe.calc_cum_data()
    #print(bass_pipe.get_max_duration(["01:20.0", "01:20.", "01:19.9",]))


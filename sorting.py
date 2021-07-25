import pandas as pd


# Sorts data by each behav code frequency using quicksort and stores it in output as .csv files O(logn) runtime
def main(data, headers):
    freq_heads = []
    for header in headers:
        if 'Freq' in header or 'freq' in header:
            freq_heads.append(header)

    sorted_dfs = {}
    for head in freq_heads:
        df = data.sort_values(by=head, kind='quicksort', ascending=False)
        sorted_dfs[head] = df
        sorted_dfs[head].to_csv('Output/' + head.replace('/', ' ') + '.csv')


if __name__ == '__main__':
    main()

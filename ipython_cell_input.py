def filter_static_values(df):
    #print(df)
    mode = df['eta'].value_counts().idxmax()
    #mode = df['eta'].mode().iloc[0]
    #print("mode: " + str(mode))
    return (df
              .groupby((df.shift()['eta'] != df['eta']).cumsum())
              .filter(lambda x: not (len(x) > 2 and x.iloc[0]['eta'] == mode))
    )
import pandas as pd

df = pd.read_csv('out_parsed', parse_dates=['actual_date', 'eta'], nrows=10e3)
filter_static_values(df)

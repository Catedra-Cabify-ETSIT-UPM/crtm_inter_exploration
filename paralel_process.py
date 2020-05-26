#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd # main data processing tool
import multiprocessing as mp # multi threading manager
import datetime # for datetime object opperations
import time # for measuring the processing time

LARGE_FILE = 'out_parsed' # path to CSV file containing the ETA records (from crtm_poll)
CHUNKSIZE = 3e6 # processing 1,000,000 rows at a time

def filter_static_values(df):
    """Remove the static ETA values from a DataFrame.
    The DataFrame must only contain information about a specific trip.
    The static values are those whose ETA value is equal to the one of the
    largest group of same consecutive ETA value and are before or after
    another row with the same ETA.
    
    Keyword arguments:
    df -- Pandas DataFrame with the ETA's
    
    Output:
    df -- filtered DataFrame
    """
    
    # Sort by actual_date and group by "bursts" of same ETA
    df_grouped = (df
                    .sort_values(['actual_date'], ascending=[False])
                    .groupby((df.shift()['eta'] != df['eta']).cumsum())
                 )
    # Get the ETA value of the largest "burst"
    mode = (df_grouped
             .get_group(
                        df_grouped
                        .size()
                        .sort_values(ascending=False)
                        .index[0]
                        )
             .iloc[0]['eta']
            )
    # Return the values that don't have the same consecutive ETA equal to the mode
    return (df_grouped
              .filter(lambda x: not (len(x) >= 2 and x.iloc[0]['eta'] == mode))
    )


def process_frame(df):
        # process data frame
        start = time.time()

        df['remaining_seconds'] = df['eta'] - df['actual_date']
        df['eta_date'] = df['eta'].dt.day
        test_df = df[df['remaining_seconds'] < pd.Timedelta(100, unit='m')]
        test_df_grouped = test_df.groupby(['cod_issue', 'cod_stop', 'cod_line', 'eta_date'])
        test_df = test_df_grouped.apply(lambda x: filter_static_values(x)).reset_index(drop=True)
        test_df = test_df[test_df['remaining_seconds'] < pd.Timedelta(1, unit='m')]
        test_df_grouped = test_df.groupby(['cod_issue', 'cod_stop', 'cod_line', 'eta_date'])
        test_arrival_times = (test_df_grouped
                               .apply(lambda x: x
                                        .sort_values(['actual_date'], ascending=[False])
                                        .iloc[0]['eta']
                                     )
                                .to_frame(name = 'arrival_time')
                                                       .reset_index()
                                                       .sort_values('arrival_time', ascending=True)
                            )
        end = time.time()
        print(end - start)
        return test_arrival_times
    
def log_result(result):
    # This is called whenever foo_pool(i) returns a result.
    # result_list is modified only by the main process, not the pool workers.
    result_list.append(result)

if __name__ == '__main__':
        reader = pd.read_csv(
                             LARGE_FILE,
                             parse_dates=['actual_date', 'eta'],
                             chunksize=CHUNKSIZE,
                             )
        pool = mp.Pool(12) # use n processes
        
        result_list = []

        for df in reader:
                # process each data frame
                pool.apply_async(process_frame, args=[df], callback = log_result)
                
        pool.close()
        pool.join()

        print("Processed chunks: " + str(len(result_list)))
        #print(pd.concat(result_list).sort_values(['arrival_time'], ascending=[True]))
        (pd
         .concat(result_list)
         .sort_values(['arrival_time'], ascending=[True])
         .to_csv('paralel_arrival_times_3e6.csv', index=False)
        )
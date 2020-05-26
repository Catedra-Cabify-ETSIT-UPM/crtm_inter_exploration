import numpy as np
from pandarallel import pandarallel
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from sqlalchemy import create_engine

# construct an engine connection string
engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
    user='postgres',
    password='lbX8hHTrTt',
    host='postgres-crtm',
    port=5432,
    database='postgres',
)

def get_running_times(cod_stop_from, cod_stop_to):
    """Get the running times between two stops for past trips"""
    # create sqlalchemy engine
    engine = create_engine(engine_string)
    trip_times_array = []

    arrival_times = pd.read_sql_query("SELECT * FROM arrival_times WHERE cod_stop = '{cod_stop_from}' OR cod_stop = '{cod_stop_to}' "
                                      .format(cod_stop_from=cod_stop_from, cod_stop_to=cod_stop_to), con=engine)
    engine.dispose()
    arrival_times_grouped = (arrival_times
                             .sort_values(by='arrival_time', ascending=True)
                             .groupby(['cod_line', 'cod_issue', 'eta_date']))

    for name, group in arrival_times_grouped:
        if (len(group) == 2):
            arrival_from = group.iloc[0]
            arrival_to = group.iloc[1]
            #print("from: " + str(arrival_from['cod_stop']) + " to: " + arrival_to['cod_stop'])
            if (
                arrival_from['cod_stop'] == cod_stop_from and
                arrival_to['cod_stop'] == cod_stop_to
            ):
                departure_time = arrival_from['arrival_time']
                arrival_time = arrival_to['arrival_time']
                trip_time = arrival_time - departure_time
                if (arrival_from['cod_line'] == arrival_to['cod_line']):
                    cod_line = arrival_from['cod_line']
                else:
                    print(
                        "cod_line from: " + str(arrival_from['cod_line']) + " cod_line to: " + arrival_to['cod_line'])
                    continue
#                 if (name[0] == '8__656___'):
#                     print("Departed at " + str(departure_time) +
#                           " and arrived at " + str(arrival_time) +
#                           " taking " + str(trip_time) + " minutes")
                trip_times_array.append([
                    cod_stop_from,
                    cod_stop_to,
                    name[0],
                    name[1],
                    departure_time,
                    arrival_time,
                    name[2],
                    trip_time,
                ])
        elif (len(group) > 2):
            print("Error: repeated cod_issue")
            print(group)

    running_times = pd.DataFrame(trip_times_array, columns=[
        'cod_stop_from',
        'cod_stop_to',
        'cod_line',
        'cod_issue',
        'departure_time',
        'arrival_time',
        'date_day',
        'trip_time',
    ])
    running_times = running_times[running_times['trip_time'] < pd.Timedelta(
        2, unit='h')]
    return running_times


def get_crtm_poll(cod_stop, cod_line):
    engine = create_engine(engine_string)
    arrival_times = pd.read_sql_query("SELECT * FROM arrival_times WHERE cod_stop = '{cod_stop}' AND cod_line = '{cod_line}'".format(cod_stop=cod_stop, cod_line=cod_line), con=engine)
    crtm_poll = pd.read_sql_query("SELECT * FROM crtm_poll "
                                  "WHERE cod_stop = '{cod_stop}' "
                                  "AND cod_line = '{cod_line}'".format(cod_stop=cod_stop,
                                                                       cod_line=cod_line),
                                  con=engine)
    crtm_poll['remaining_seconds_est'] = crtm_poll['eta'] - crtm_poll['actual_date']
    crtm_poll['eta_date'] = crtm_poll['eta'].dt.day
    
    crtm_poll_grouped = crtm_poll.groupby(['cod_issue', 'cod_stop', 'cod_line', 'eta_date'])
    crtm_poll_filtered = crtm_poll_grouped.parallel_apply(lambda x: add_static_column(x)).reset_index(drop=True)
    
    crtm_poll_filtered['arrival_time'] = crtm_poll_filtered.parallel_apply(get_arrival_time, arrival_times=arrival_times, axis=1)
    crtm_poll_filtered.dropna(inplace=True)
    
    crtm_poll_filtered['remaining_seconds'] = (crtm_poll_filtered['arrival_time'] - crtm_poll_filtered['actual_date']).astype('timedelta64[s]')
    crtm_poll_filtered['error'] = (crtm_poll_filtered['arrival_time'] - crtm_poll_filtered['eta']).astype('timedelta64[s]')
    
    #crtm_poll_filtered = crtm_poll_filtered[crtm_poll_filtered['error'] < 1500] # error<25min
    crtm_poll_filtered = filter_outliers_df(crtm_poll_filtered, 'error')
    crtm_poll_filtered = crtm_poll_filtered[crtm_poll_filtered['remaining_seconds'] >= 0]
    crtm_poll_filtered = crtm_poll_filtered[crtm_poll_filtered['remaining_seconds'] <= 90*60]
    
    return crtm_poll_filtered
    
    
def get_arrival_time(row, arrival_times):
    cod_line = row['cod_line']
    cod_issue = row['cod_issue']
    eta_date = row['eta_date']

    selected_arrival_times = arrival_times[(arrival_times['cod_line'] == cod_line) &
                                           (arrival_times['cod_issue'] == cod_issue) &
                                           (arrival_times['eta_date']
                                            == eta_date)
                                           ]['arrival_time']

    if (len(selected_arrival_times.index) == 1):
        arrival_time = selected_arrival_times.iloc[0]
    else:
        arrival_time = None

    return arrival_time


def filter_static_values(df):
    """Remove the static ETA values from a DataFrame.
    The DataFrame must only contain information about a specific trip.
    The static values are those whose ETA value is equal to the one of the
    prevoius row.
    
    Keyword arguments:
    df -- Pandas DataFrame with the ETA's
    
    Output:
    df -- filtered DataFrame
    """
    
    # Return the values that don't have the same consecutive ETA
    return df.loc[(df['eta'] != df.shift(-1)['eta']) & (df['eta'] != df.shift(1)['eta'])]


def add_static_column(df):
    """Adds a column to the DataFrame with a boolean value
    depending on if the row's ETA was marked as static or not."""
    df['static'] = (((df['eta'] != df.shift(-1)['eta']) &
                     (df['eta'] != df.shift(1)['eta']))
                    .apply(lambda x: 0 if x else 1))

    return df



def filter_outliers_df(df, col):
    """Filter out rows with outlier values in the given column"""
    col_values = df[col]
    # Computing IQR
    Q1 = col_values.quantile(0.25)
    Q3 = col_values.quantile(0.75)
    IQR = Q3 - Q1
    #print(Q1 - 1.5*IQR)
    #print(Q3 + 1.5*IQR)
    return df[(Q1 - 1.5*IQR <= col_values) & (col_values <= Q3 + 1.5*IQR)]
    

def filter_outliers(running_times):
    """Filter out the outlier values for the trip_time columns"""
    return filter_outliers_df(running_times, 'trip_time')


# Read the Madrid working days calendar
calendar = pd.read_csv('./common/madrid_working_days.csv',
                       sep=';',
                       parse_dates=['Dia'],
                       usecols=[0, 2]
                       )
calendar.columns = ['day', 'type']
calendar['type'] = calendar['type'].apply(
    lambda x: 'festivo' if x == 'domingo' else x)

def get_day_type(date):
    """Get the day type (working, festive...) for a given date"""
    day_type = (calendar[calendar['day'].apply(
        lambda x: x.date()) == date.date()]['type'].iloc[0])

    if (day_type == 'laborable' and date.weekday() == 4):
        day_type = 'viernes'
    
    day_type_translation = {
        'laborable': 'working',
        'festivo': 'festive',
        'sábado': 'saturday',
        'viernes': 'friday'
    }

    return day_type_translation[day_type]


def get_day_type_bool(date):
    """Get if a date is a festive"""
    day_type = (calendar[calendar['day'].apply(lambda x: x.date()) == date.date()].iloc[0]['type'])

    if day_type == 'festivo':
        day_type = 1
    else:
        day_type = 0
    
    return day_type


def get_day_time_normalized(date):
    """Get a normalized value [-1,1) for the date hour"""
    seconds_since_midnight = (date - date.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
    time_normalized = seconds_since_midnight/(24*60*60)
    return (2*time_normalized - 1)


def plot_graphs(plot_df):
    """Plot different metrics"""
    histogram_error(plot_df).show()
    boxplot_through_the_day(plot_df).show()
    boxplot_error_through_the_day(plot_df).show()
    
    
def histogram_error(plot_df):
    """Returns a plot with the histogram and boxplot of the estimation error"""
    fig = px.histogram(plot_df,
                       x=plot_df['error']/60,
                       color='cod_line',
                       marginal="box",
                       histfunc='count',
                       nbins=50)

    # Edit the layout
    fig.update_layout(title='Estimation error: histogram and boxplot',
                      xaxis_title='Error: (running_time - running_time_estimate) (minutes)',
                      yaxis_title='Count')
    return fig
    
    
def boxplot_through_the_day(plot_df):
    """Returns a plot with the boxplot of the estimations through the day"""
    fig = go.Figure()

    fig.add_trace(
        go.Box(
            x=plot_df['departure_time'].dt.hour,
            y=plot_df['trip_time_estimate'].dt.total_seconds()/60,
            name='running time estimate',
            marker=dict(
                color='red',
            ),
        )
    )

    fig.add_trace(
        go.Box(
            x=plot_df['departure_time'].dt.hour,
            y=plot_df['trip_time'].dt.total_seconds()/60,
            name='running time',
            marker=dict(
                color='green',
            ),
        )
    )

    #Edit the layout
    fig.update_layout(title='Estimated running times and running times through the day',
                      xaxis_title='Hour of the day',
                      yaxis_title='Running time (minutes)')
    return fig


def boxplot_error_through_the_day(plot_df):
    """Returns a plot with the boxplot of the estimation error through the day"""
    fig = px.box(x=plot_df['departure_time'].dt.hour,
               y=plot_df['error']/60,
               color=plot_df['cod_line'],
               points="all",
               hover_data=[
                   plot_df['departure_time'].dt.day_name(),
                   plot_df['departure_time'].dt.minute,
                   plot_df['departure_time'].dt.day
               ]
              )
    #Edit the layout
    fig.update_layout(title='Running time estimation error through the day',
                      xaxis_title='Hour of the day',
                      yaxis_title='Running time error (minutes)')

    return fig


def get_metrics(plot_df):
    """Return several metrics for the DataFrame"""
    print("MAE: " + str(np.mean(
        np.abs(
            plot_df['error']/60
        ))) + " minutes.")
    print("MAPE: " + str(np.mean(
        np.abs(
            (plot_df['error']/60)/(plot_df['trip_time'].dt.total_seconds()/60)
        ))*100) + " %.")
    print("RMSE: " + str(np.sqrt(np.mean(np.square(plot_df['error']/60)))) + " minutes.")
    
    print(plot_df[['trip_time', 'trip_time_estimate', 'error']].describe())
    

def get_best_score(grid):
    """Get the best estimator score"""
    best_score = -grid.best_score_
    print(best_score)    
    print(grid.best_params_)
    print(grid.best_estimator_)
    
    return best_score


def get_day_time(date):
    """Get day time in decimal format"""
    seconds_since_midnight = (date - date.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
    time_normalized = seconds_since_midnight/(24*60*60)
    return (24*time_normalized)

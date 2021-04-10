# from matplotlib import pyplot as plt
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_table
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import time
import subprocess

from analysis import get_words, SparkSession
cc = pd.read_csv("countries_codes_and_coordinates.txt")


def get_long_lat(df, cc):
    long = {}
    lat = {}
    alpha = {}
    country_name = {}
    for x in df.country:
        if x == "??":
            long[0.0] = x
            lat[0.0] = x
            alpha["ATA"] = x  # Assign to Antartida
            country_name["No info"] = x
        else:
            idx = cc["Alpha-2code"] == x
            long[cc["Longitude"].loc[idx].iloc[0]] = x
            lat[cc["Latitude"].loc[idx].iloc[0]] = x
            alpha[cc["Alpha-3code"].loc[idx].iloc[0]] = x
            country_name[cc["Country"].loc[idx].iloc[0]] = x
    df["Longitude"] = long
    df["Latitude"] = lat
    df["alpha"] = alpha
    df["country"] = country_name
    return df


def my_plot(df):
    fig = px.scatter_geo(df, locations="alpha",
                         color="avg_polarity",
                         color_continuous_scale="balance",  # jet/balance
                         color_continuous_midpoint=0,
                         range_color = [-1, 1],
                         hover_name="country",  # column added to hover information
                         size="avg_subjectivity",  # size of markers
                         size_max=12,
                         projection="natural earth",
                         hover_data=[
                             "tweets_by_country",
                             "avg_polarity", "std_polarity",
                             "avg_subjectivity", "std_subjectivity"
                         ],
                         text="tweets_by_country",
                         title="Reaction in twitter over the world",
                         )
    return fig


def load_dfs(spark, cc):
    print("Loading df.")
    if spark == "":
        df = pd.read_csv("df.csv")\
            .rename(columns={"avg(CAST(polarity AS DOUBLE))": "avg_polarity"})
        del df["Unnamed: 0"]
    else:
        df = spark.sql("select * from alltweets").toPandas()
        print(df)
    df = get_long_lat(df, cc)
    return df


def create_dash():
    df = load_dfs("", cc)
    app = dash.Dash(__name__)
    app.layout = html.Div([
        dcc.Graph(id="map"),
        html.Button('Refresh', id='refresh', n_clicks=0),
        dash_table.DataTable(
            id='table',
            columns=[{"name": i, "id": i} for i in df.columns],
            data=df.to_dict('records'),
        ),
    ])
    return app


app = create_dash()


@app.callback(
    Output("map", "figure"),
    Output("table", "data"),
    [dash.dependencies.Input('refresh', 'n_clicks')]
    )
def refresh_data(color):
    df = load_dfs(spark, cc)
    fig = my_plot(df)
    table = df.to_dict('records')
    return fig, table


def start_spark():
    print("Starting spark session")
    spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()
    words = get_words(spark)
    print("Starting WriteStream")
    query = words.writeStream.outputMode("complete").format("memory").queryName("alltweets") \
        .trigger(processingTime='60 seconds').start()

    # query2 = words.writeStream.queryName("all_tweets2") \
    #     .outputMode("update").format("console") \
    #     .trigger(processingTime='60 seconds').start()
    print("WriteStream started")
    # query.awaitTermination()
    time.sleep(120)
    return spark


if __name__ == '__main__':
    keyword = "prince"
    subprocess.Popen(["python3", "connecting_twitter.py", "-k", keyword])
    spark = start_spark()
    app.run_server(debug=False)
    #
    # spark = ""
    # app.run_server(debug=True)


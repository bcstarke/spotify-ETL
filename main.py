import json
import pandas as pd
import sqlalchemy
import requests
from datetime import datetime
import datetime
import sqlite3
import threading

DATABASE_LOCATION = 'sqlite:///my_played_tracks.sqlite'
USER_ID = 'benstarke'
TOKEN = 'BQAgNrpEriMa3S5w6Sb-Uc4VKKx4foDznrCygBLAj4rUGAIWeqwduw_DnOoO-3YxDTjXe_emrQJxflmq74Avbun3k6AATiQkgqO5XbXVlSxvhmLNgFXjKZgBel_CKNA-AqPSHWmjrh-K4tE'



def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finish execution")
        return False

    #Primary key check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary key check is violated")

    # Check for any null values
    if df.isnull().values.any():
        raise Exception("Null value found")

    # Check all time stamps are from yesterday's date
    # yesterday = datetime.datetime.now() - datetime.timedelta(days=90)
    # yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    #
    # timestamps = df["timestamp"].tolist()
    # for timestamp in timestamps:
        # if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            # raise Exception("At least one time was not yesterday")
    # return True


def main_func():
    headers = {
        "Accept": "application/json",
        "Content-type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=10)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(
        time=yesterday_unix_timestamp), headers=headers)

    data = r.json()
    # print(data)

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []
    # print(data)

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    # print(timestamps)
    # print(song_names)
    # print(song_names)
    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])

    if check_if_valid_data(song_df):
        print("Data valid, proceeding with Load stage")
    # print(song_df)

    # create engine and pass database location
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)

    # initiate connection
    conn = sqlite3.connect("my_played_tracks.sqlite")

    # create cursor
    cursor = conn.cursor()

    sql_query = """
            CREATE TABLE IF NOT EXISTS my_played_tracks (
                song_name VARCHAR(200),
                artist_name VARCHAR(200),
                played_at VARCHAR(200),
                timestamp VARCHAR(200),
                CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
            )
        """

    cursor.execute(sql_query)
    print("Opened DB successfully")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists="append")
    except:
        print("Data already exists")
    conn.close()
    print("Closed DB successfully")
    return 0


if __name__ == "__main__":
    # while True:
    # threading.Timer(5.0, main_func()).start()  # called every minute
    main_func()

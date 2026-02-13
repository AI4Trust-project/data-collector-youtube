import os
import json

import pandas as pd
import datetime as dt

from trino.dbapi import connect
from trino.exceptions import TrinoQueryError
from kafka import KafkaProducer


import urllib.request

TRINO_HOST = os.environ.get("TRINO_HOST")
TRINO_USER = os.environ.get("TRINO_USER")

API_KEY = os.environ.get("API_KEY")
API_URL = os.environ.get("API_URL")

VIEW_FACTOR = 1
LIKE_FACTOR = 1
COMMENT_FACTOR = 1
TOP_N = 100

LANGUAGE_CODES = {
    "en": "EN",
    "fr": "FR",
    "es": "ES",
    "de": "DE",
    "el": "EL",
    "it": "IT",
    "pl": "PL",
    "ro": "RO",
}

mapping_df = {
    "vaccin*": "Health Care",
    "cambio climático": "Climate Change",
    "climate change": "Climate Change",
    "refugee*": "Migrants",
    "global warming": "Climate Change",
    "*migra*": "Migrants",
    "changement climatique": "Climate Change",
    "Ausländer": "Migrants",
    "Klimawandel": "Climate Change",
    "cambiamento climatico": "Climate Change",
    "calentamiento global": "Climate Change",
    "réfugié*": "Migrants",
    "réchauffement climatique": "Climate Change",
    "impf*": "Health Care",
    "Flüchtling*": "Migrants",
    "κλιματική αλλαγή": "Climate Change",
    "riscaldamento globale": "Climate Change",
    "schimbări climatice": "Climate Change",
    "zmiana klimatu": "Climate Change",
    "geimpft": "Health Care",
    "*migr*": "Migrants",
    "globalne ocieplenie": "Climate Change",
    "globale Erwärmung": "Climate Change",
    "Vakzin*": "Health Care",
    "climate": "Climate Change",
    "vacun*": "Health Care",
    "υπερθέρμανση του πλανήτη": "Climate Change",
    "refugiat*": "Migrants",
    "*szczepi*": "Health Care",
    "încălzire globală": "Climate Change",
    "εμβολι*": "Health Care",
    "refugiad*": "Migrants",
    "*μεταναστ*": "Migrants",
    "εμβόλι*": "Health Care",
    "rifugiat*": "Migrants",
    "uchodźc*": "Migrants",
}

list_non_relevant = [
    "Music",
    "Entertainment",
    "Music_of_Latin_America",
    "Film",
    "Humour",
    "Rock_music",
    "Pop_music",
    "Video_game_culture",
    "Hip_hop_music",
    "Hobby",
    "Role-playing_video_game",
    "Action_game",
    "Electronic_music",
    "Action-adventure_game",
    "Independent_music",
    "Sport",
    "Tourism",
    "Pet",
    "Music_of_Asia",
    "Vehicle",
    "Cricket",
    "Performing_arts",
    "Soul_music",
    "Strategy_video_game",
    "Food",
    "Reggae",
    "Simulation_video_game",
    "Association_football",
    "Rhythm_and_blues",
    "Physical_fitness",
    "Jazz",
    "Country_music",
    "Casual_game",
    "Christian_music",
    "Puzzle_video_game",
    "Fashion",
    "Classical_music",
    "Professional_wrestling",
    "Boxing",
    "Music_video_game",
    "Racing_video_game",
    "Volleyball",
    "Motorsport",
    "Mixed_martial_arts",
    "Sports_game",
    "Golf",
    "Baseball",
    "Physical_attractiveness",
    "Tennis",
    "Basketball",
]


def fetch_videos(conn, window_start, window_end):
    print("Query data lake for videos data...")
    try:
        cur = conn.cursor()
        # select via group by only a single entry per video, with max(created_at)
        query = (
            "SELECT v1.video_id, v1.publish_time, v1.published_at, v1.created_at, v1.relevance_language, v1.id, v1.description, v1.search_id,"
            + "v1.data_owner, v1. title, v1.image_url, v1.video_url, v1.keyword, v1.keyword_id, v1.topic FROM youtube.videos v1 "
            + " join (select video_id, max(_timestamp) as _timestamp from youtube.videos group by video_id) v2 "
            + " on v1.video_id = v2.video_id and v1._timestamp = v2._timestamp where v1._timestamp between ? and ?"
        )
        rows = cur.execute(
            query, [int(window_start * 1000), int(window_end * 1000)]
        ).fetchall()
        columns = [c[0] for c in cur.description]
        df_raw = pd.DataFrame(
            rows,
            columns=columns,
        ).drop_duplicates(subset="video_id", keep="first")
        df_raw["published_at"] = pd.to_datetime(df_raw["published_at"])
        # transform topics
        df_raw["topic"] = df_raw["topic"].apply(
            lambda x: x.replace(" ", "_") if x is not None else None
        )

        return df_raw
    finally:
        cur.close()


def fetch_video_topics(conn, window_start, window_end, ignore_topics=None):
    print("Query data lake for videos data...")
    try:
        cur = conn.cursor()
        # select via group by only a single entry per video, with max(created_at)
        query = (
            "SELECT v1.video_id,v1.created_at, v1.topic_categories FROM youtube.video_topic_details v1 "
            + " join (select video_id, max(_timestamp) as _timestamp from youtube.video_topic_details group by video_id) v2 "
            + " on v1.video_id = v2.video_id and v1._timestamp = v2._timestamp where v1._timestamp between ? and ?"
        )

        if ignore_topics is not None:
            igt = [
                "https://en.wikipedia.org/wiki/" + x if x[:8] != "https://" else x
                for x in ignore_topics
            ]
            #  filter by checking array
            query = (
                query + " AND ANY_MATCH(v1.topic_categories, c -> NOT CONTAINS(?, c))"
            )

        rows = cur.execute(
            query, [int(window_start * 1000), int(window_end * 1000), igt]
        ).fetchall()

        columns = [c[0] for c in cur.description]
        df = pd.DataFrame(
            rows,
            columns=columns,
        )

        # remove namespace from topics
        df["topics"] = df["topic_categories"].apply(
            lambda urls: [url.rstrip("/").split("/")[-1] for url in urls]
        )

        return df
    finally:
        cur.close()


def analyze_engagement(conn, df_videos_clean, window_start, window_end):
    print("Analyze engagement...")

    try:
        cur = conn.cursor()
        query = (
            "SELECT v1.video_id, v1.view_count, v1.comment_count, v1.like_count, v1.favorite_count, v1.created_at FROM youtube.video_statistics v1 "
            + " join (select video_id, max(_timestamp) as _timestamp from youtube.video_statistics group by video_id) v2 "
            + " on v1.video_id = v2.video_id and v1._timestamp = v2._timestamp where v1._timestamp between ? and ?"
        )

        stats = cur.execute(
            query, [int(window_start * 1000), int(window_end * 1000)]
        ).fetchall()
        df_video_stats = (
            pd.DataFrame(
                stats,
                columns=[
                    "video_id",
                    "view_count",
                    "comment_count",
                    "like_count",
                    "favorite_count",
                    "created_at",
                ],
            )
            .sort_values(by="created_at", ascending=False)
            .drop_duplicates(subset="video_id", keep="first")
        )

        df_videos_complete = df_videos_clean.merge(df_video_stats, on="video_id")
        df_videos_complete["like_count"] = (
            pd.to_numeric(
                df_videos_complete["like_count"], errors="coerce", downcast="integer"
            )
            .fillna(0)
            .astype("int")
        )
        df_videos_complete["view_count"] = (
            pd.to_numeric(
                df_videos_complete["view_count"], errors="coerce", downcast="integer"
            )
            .fillna(0)
            .astype("int")
        )
        df_videos_complete["comment_count"] = (
            pd.to_numeric(
                df_videos_complete["comment_count"], errors="coerce", downcast="integer"
            )
            .fillna(0)
            .astype("int")
        )
        df_videos_complete["favorite_count"] = (
            pd.to_numeric(
                df_videos_complete["favorite_count"],
                errors="coerce",
                downcast="integer",
            )
            .fillna(0)
            .astype("int")
        )
        return df_videos_complete
    except TrinoQueryError as e:
        print(f"Query error: {e}")
    finally:
        cur.close()


def init_context(context):
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_BROKER")],
        key_serializer=lambda x: x.encode("utf-8"),
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    setattr(context, "producer", producer)


def handler(context, event):
    body = event.body.decode("utf-8")
    producer = context.producer
    now = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    ncount = TOP_N

    if body:
        parameters = json.loads(body)
        if "date" in parameters:
            now = dt.datetime.strptime(parameters["date"], "%Y-%m-%d").replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        if "top_n" in parameters:
            ncount = parameters["top_n"]

    try:
        conn = connect(
            host=TRINO_HOST,
            user=TRINO_USER,
            catalog="iceberg",
        )
        conn_db = connect(
            host=TRINO_HOST,
            user=TRINO_USER,
            catalog="db",
        )

        # 1 day interval
        window_end = (now - dt.timedelta(seconds=1)).timestamp()
        window_start = (now - dt.timedelta(days=1)).timestamp()

        print(f"Run analyze on interval {window_start} - {window_end}")
        dft = fetch_video_topics(conn, window_start, window_end, list_non_relevant)
        print(f"Found  {str(len(dft))} relevant videos.")

        print(f"Fetch statistics...")
        dfs = analyze_engagement(conn, dft, window_start, window_end)

        print(f"Collect all videos data for interval  {window_start} - {window_end}...")
        dfv = fetch_videos(conn, window_start, window_end)
        print(f"Found  {str(len(dfv))} videos.")

        print(f"Merge data and calculate score...")
        dfe = dfs.merge(dfv, on="video_id")
        dfe["score"] = (
            (dfe["view_count"] * VIEW_FACTOR)
            + (dfe["comment_count"] * COMMENT_FACTOR)
            + (dfe["like_count"] * LIKE_FACTOR)
        )

        print(f"Pick top {str(ncount)} from {str(len(dfe))} videos....")
        topn = dfe.sort_values(by="score", ascending=False).head(ncount)[
            [
                "video_id",
                "id",
                "search_id",
                "relevance_language",
                "data_owner",
                "title",
                "description",
                "image_url",
                "video_url",
                "publish_time",
                "created_at",
                "keyword",
                "keyword_id",
                "topic",
                "comment_count",
                "like_count",
                "view_count",
                "favorite_count",
            ]
        ]

        topn["relevance_language"] = topn["relevance_language"].apply(
            lambda x: LANGUAGE_CODES.get(x.lower()) if x is not None else None
        )

        list = topn.to_dict(orient="records")
        print(f"Send {str(len(list))} videos to pipe....")

        for r in list:
            raw = json.dumps(r)
            id = r["search_id"] + "|" + r["id"]
            value = json.loads(raw) | {
                "id": id,
            }
            data = json.dumps(value)

            print(f"Send video {value['id']}...")

            context.producer.send(
                "pipe.youtube",
                key=id,
                value=r,
            )

            try:
                req = urllib.request.Request(API_URL, data=data.encode("utf-8"))
                req.add_header("Content-Type", "application/json")
                req.add_header("X-API-KEY", API_KEY)
                req.get_method = lambda: "POST"

                with urllib.request.urlopen(req) as f:
                    response = f.read().decode("utf-8")
                    print(f"\tResponse from api {response}")

            except Exception as e:
                context.logger.error(f"Error calling API: {e}")

    except Exception as e:
        context.logger.error(f"Error with processing: {e}")
    finally:
        conn.close()
        conn_db.close()

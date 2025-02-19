import json
import os
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from stringcase import snakecase
import time

import psycopg2
from googleapiclient.discovery import build

from kafka import KafkaProducer

DBNAME = os.environ.get("DATABASE_NAME")
USER = os.environ.get("DATABASE_USER")
PASSWORD = os.environ.get("DATABASE_PWD")
HOST = os.environ.get("DATABASE_HOST")
PORT = os.environ.get("DATABASE_PORT")
DELAY = 15


def init_context(context):
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_BROKER")],
        key_serializer=lambda x: x.encode("utf-8"),
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    api_key = os.environ.get("YOUTUBE_API_KEY")
    youtube = build("youtube", "v3", developerKey=api_key)

    setattr(context, "producer", producer)
    setattr(context, "youtube", youtube)


def wait_until_midnight():
    # Get the current time in PDT
    now = datetime.now(timezone(timedelta(hours=-7)))

    # Calculate the time until the next midnight PDT
    midnight_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    midnight_tomorrow = midnight_today + timedelta(days=1)

    # Check if midnight has already passed today
    if now > midnight_today:
        # Wait until tomorrow midnight
        wait_time = (midnight_tomorrow - now).total_seconds()
    else:
        # Wait until tonight's midnight
        wait_time = (midnight_today - now).total_seconds()

    print("WAITING UNTIL MIDNIGHT PDT")

    # Wait for the calculated time
    time.sleep(wait_time)


def search_keyword(search_info: dict, context):
    context.logger.info("Call youtube search api for " + search_info["keyword"])
    responses = []
    try:
        nxPage = "start"
        part = ["snippet", "id"]

        # search keyword
        while nxPage != "":
            videos_response = {}

            if nxPage == "start":
                videos_response = (
                    context.youtube.search()
                    .list(
                        part=part,
                        q=search_info["keyword"],
                        maxResults=search_info["max_results"],
                        order=search_info["order"],
                        safeSearch=search_info["safe_search"],
                        relevanceLanguage=search_info["relevance_language"],
                        type=search_info["type"],
                        regionCode=search_info["region_code"],
                    )
                    .execute()
                )
            else:
                videos_response = (
                    context.youtube.search()
                    .list(
                        part=part,
                        q=search_info["keyword"],
                        maxResults=search_info["max_results"],
                        order=search_info["order"],
                        safeSearch=search_info["safe_search"],
                        relevanceLanguage=search_info["relevance_language"],
                        type=search_info["type"],
                        regionCode=search_info["region_code"],
                        pageToken=nxPage,
                    )
                    .execute()
                )

            responses.append(videos_response)

            if "nextPageToken" in videos_response.keys():
                nxPage = videos_response["nextPageToken"]
                search_info["pages"] += 1
            else:
                nxPage = ""
                search_info["pages"] += 1
    except Exception as e:
        print("YT SEARCH: ERROR SEARCH KEYWORD")
        context.logger.error(str(e))
        if "quota" in str(e).lower():
            wait_until_midnight()

    # flatten response from pages
    return [item for row in responses for item in row["items"]]


def get_keywords(context, conn, kid):
    cur = None

    try:
        cur = conn.cursor()
        query = (
            "SELECT keyword, relevance_language, region_code,"
            " max_results, safe_search, keyword_id FROM youtube.search_keywords"
        )
        if kid and kid > 0:
            query = (
                "SELECT keyword, relevance_language, region_code,"
                " max_results, safe_search, keyword_id FROM youtube.search_keywords"
                " WHERE keyword_id='" + str(kid) + "'"
            )

        cur.execute(query)
        row = cur.fetchall()

        return row if row else []
    except Exception as e:
        context.log.error(f"ERROR FIND KEYWORDS: {e}")
    finally:
        cur.close()

    return []


def handler(context, event):

    body = event.body.decode("utf-8")

    # init search
    dataOwner = "FBK"
    kid = None

    if body:
        parameters = json.loads(body)
        if "keyword_id" in parameters:
            kid = int(parameters["keyword_id"])

    context.logger.info("Run search for " + dataOwner)
    conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASSWORD, host=HOST)
    keywords = get_keywords(context, conn, kid)

    for (
        keyword,
        relevanceLanguage,
        regionCode,
        maxResults,
        safeSearch,
        keywordId,
    ) in keywords:
        query_uuid = str(uuid.uuid4())
        context.logger.info("Search for " + keyword + "id " + query_uuid)
        date = datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        search_info = {
            "id": query_uuid,
            "data_owner": dataOwner,
            "created_at": date,
            "keyword": keyword,
            "keyword_id": keywordId,
            "max_results": maxResults,
            "order": "viewCount",
            "safe_search": safeSearch,
            "relevance_language": relevanceLanguage,
            "type": "video",
            "region_code": regionCode,
            "pages": 0,
        }
        context.logger.debug(search_info)

        responses = search_keyword(search_info=search_info, context=context)

        context.logger.info("Responses for " + keyword + " " + str(len(responses)))

        if len(responses) > 0:
            # push search details to persist in iceberg
            row = dict(search_info)
            row["responses"] = len(responses)
            m = json.loads(json.dumps(row))
            k = m["id"] + "|" + m["keyword_id"]

            context.logger.info("Push search details to topic for id " + query_uuid)
            context.producer.send("youtube.searches", key=k, value=m)

            # iterate over responses to send 1 msg per video
            # collect video IDs and send to be collected
            context.logger.info(
                "Process " + str(len(responses)) + " videos for search " + query_uuid
            )
            context.logger.debug(str(responses))
            for item in responses:
                try:
                    id = str(uuid.uuid4())
                    detail = {snakecase(k): v for k, v in item["id"].items()}

                    # skip thumbnails
                    snippet = {
                        snakecase(k): v for k, v in dict(item["snippet"]).items()
                    }
                    snippet.pop("thumbnails", None)
                    snippet.pop("live_broadcast_content", None)

                    # keep one
                    image_url = None
                    if "high" in item["snippet"].get("thumbnails"):
                        image_url = item["snippet"]["thumbnails"]["high"]["url"]

                    # store result + info + (text) snippets
                    row = (
                        detail
                        | {
                            "data_owner": search_info["data_owner"],
                            "created_at": search_info["created_at"],
                            "keyword": search_info["keyword"],
                            "keyword_id": search_info["keyword_id"],
                            "search_id": search_info["id"],
                            "relevance_language": search_info["relevance_language"],
                            "region_code": search_info["region_code"],
                            "id": id,
                        }
                        | snippet
                        | {
                            "image_url": image_url,
                            "video_url": "https://www.youtube.com/watch?v={}".format(
                                detail["video_id"]
                            ),
                        }
                    )

                    m = json.loads(json.dumps(row))
                    k = m["search_id"] + "|" + m["id"]

                    # send data to be collected
                    context.logger.info(
                        "Push video details to topic for id " + row["video_id"]
                    )
                    context.producer.send("youtube.videos", key=k, value=m)
                    # # send data to be merged
                    # context.producer.send("youtube.youtuber-merger", value=m)
                except Exception as e:
                    context.logger.error(f"Error with response: {e}")
                    continue

        # wait to stagger requests
        time.sleep(DELAY)

    # close connection
    conn.close()

    return context.Response(
        body=f"Run search",
        headers={},
        content_type="text/plain",
        status_code=200,
    )

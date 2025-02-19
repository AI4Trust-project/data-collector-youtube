import json
import os
import time
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from stringcase import snakecase

from googleapiclient.discovery import build

from kafka import KafkaProducer


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


def write_comment(context, base, thread_id, item):
    try:
        comment = dict(item)
        comment["snippet"].pop("authorChannelId", None)

        row = (
            base.copy()
            | {"id": str(uuid.uuid4())}
            | {
                "etag": comment["etag"],
                "comment_id": comment["id"],
                "thread_id": thread_id,
            }
            | {snakecase(k): v for k, v in comment["snippet"].items()}
        )

        m = json.loads(json.dumps(row))
        k = m["search_id"] + "|" + m["content_id"] + "|" + m["id"]

        context.logger.debug("Write comment " + comment["id"])
        context.producer.send("youtube.video_comments", key=k, value=m)
    except Exception as e:
        print("Error writing: {}".format(e))


def handler(context, event):
    data = json.loads(event.body.decode("utf-8"))

    searchId = data["search_id"]
    contentId = data["id"]
    videoId = data["video_id"]

    context.logger.info(
        "handle message for video {}:{}-{}".format(searchId, contentId, videoId)
    )

    date = datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    dataOwner = "FBK"

    parts = ["id", "snippet", "replies"]

    #  base content for all metadata
    baserow = {
        "search_id": searchId,
        "content_id": contentId,
        "video_id": videoId,
        "data_owner": dataOwner,
        "created_at": date,
    }

    # iterate in the comment pages
    nxPage = "start"
    response = False
    textFormat = "plainText"
    maxResults = 100
    order = "time"
    pages = 0

    while nxPage != "":
        context.logger.debug(f"Fetch page {nxPage} for {videoId}")

        try:
            if nxPage == "start":
                comment_threads = (
                    context.youtube.commentThreads()
                    .list(
                        part=parts,
                        videoId=videoId,
                        textFormat=textFormat,
                        maxResults=maxResults,
                        order=order,
                    )
                    .execute()
                )
            else:
                comment_threads = (
                    context.youtube.commentThreads()
                    .list(
                        part=parts,
                        videoId=videoId,
                        textFormat=textFormat,
                        maxResults=maxResults,
                        order=order,
                        pageToken=nxPage,
                    )
                    .execute()
                )

            context.logger.debug(
                f"Fetched page {nxPage} for {videoId} items "
                + str(len(comment_threads["items"]))
            )

            for item in comment_threads["items"]:
                # insert comments on iceberg
                try:
                    # write comment thread details
                    # pop tlc, we'll write as comment later
                    comment = item["snippet"].pop("topLevelComment", None)

                    row = (
                        baserow.copy()
                        | {"id": str(uuid.uuid4())}
                        | {"etag": item["etag"], "thread_id": item["id"]}
                        | {snakecase(k): v for k, v in item["snippet"].items()}
                    )

                    m = json.loads(json.dumps(row))
                    k = m["search_id"] + "|" + m["content_id"] + "|" + m["thread_id"]

                    context.logger.debug("Write thread comment " + m["id"])
                    context.producer.send(
                        "youtube.video_comment_threads", key=k, value=m
                    )

                    # top level comment from snippet
                    if comment:
                        write_comment(context, baserow, item["id"], comment)

                    # all replies
                    if "replies" in item:
                        for reply in item["replies"]["comments"]:
                            write_comment(context, baserow, item["id"], reply)

                except Exception as e:
                    print("Error: {}".format(e))

            if "nextPageToken" in comment_threads.keys():
                nxPage = comment_threads["nextPageToken"]
                pages += 1
            else:
                nxPage = ""
                pages += 1

            response = True

        except Exception as e:
            nxPage = ""
            response = False
            context.logger.error(f"Error: {e}")
            if "quota" in str(e).lower():
                wait_until_midnight()

    return response

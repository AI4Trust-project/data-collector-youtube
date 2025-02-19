import json
import os
import time
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


def handler(context, event):
    data = json.loads(event.body.decode("utf-8"))

    search_id = data["search_id"]
    id = data["id"]
    video_id = data["video_id"]

    context.logger.info(
        "handle message for video {}:{}-{}".format(search_id, id, video_id)
    )

    date = datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    dataOwner = "FBK"

    #  call youtube API for all details on video
    parts = [
        "contentDetails",
        "localizations",
        "snippet",
        "statistics",
        "status",
        "topicDetails",
    ]
    response = False

    context.logger.info("Collect metadata for " + video_id + " " + str(parts))

    try:
        videos_response = (
            context.youtube.videos()
            .list(
                part=parts,
                id=video_id,
            )
            .execute()
        )

        response = True

    except Exception as e:
        print(e)
        if "quota" in str(e).lower():
            wait_until_midnight()

    if response:
        # parse response
        items = videos_response["items"][0]

        #  base content for all metadata
        baserow = {
            "id": id,
            "search_id": search_id,
            "video_id": video_id,
            "data_owner": dataOwner,
            "created_at": date,
        }

        for part in parts:
            context.logger.info("Process metadata for " + video_id + ": " + part)

            # to table
            try:
                row = {snakecase(k): v for k, v in items[part].items()} | baserow.copy()

                m = json.loads(json.dumps(row))
                k = m["search_id"] + "|" + m["id"]
                context.producer.send(
                    "youtube.video_" + snakecase(part), key=k, value=m
                )
            except Exception as e:
                print("Error writing {}: {}".format(part, e))

import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
import boto3
import botocore
from io import BytesIO
import base64
import requests
from textwrap import wrap

from kafka import KafkaProducer

BASE_PATH = "youtube/artifacts/thumbnails/"
S3_BUCKET = os.environ.get("S3_BUCKET")
THUMB_TYPES = [
    # "default",
    # "mqdefault",
    # "sddefault",
    # "hqdefault",
    # "sddefault",
    # "maxresdefault",
]


def init_context(context):
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_BROKER")],
        key_serializer=lambda x: x.encode("utf-8"),
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    setattr(context, "producer", producer)

    s3c = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=None,
        config=boto3.session.Config(signature_version="s3v4"),
        verify=False,
    )
    setattr(context, "s3c", s3c)


def key_exists(client, bucket, key):
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise


def fetch_image(image_url, s3c):

    # Fetch the image from the image URL
    response = requests.get(image_url)
    if response.status_code == 200:
        # Open the image as byte array
        img_byte_arr = BytesIO(response.content)
        img_bytes = img_byte_arr.getvalue()

        # Compute SHA hash of the image content
        img_hash = hashlib.sha256(img_bytes)
        img_sha = img_hash.hexdigest()

        # Upload
        img_path = BASE_PATH + "/".join(wrap(img_sha[:12], 4)) + "/" + img_sha

        # dedup upload
        if not key_exists(s3c, S3_BUCKET, img_path):
            # upload with checksum enabled
            s3c.put_object(
                Body=img_bytes,
                Bucket=S3_BUCKET,
                Key=img_path,
                ContentType=response.headers.get("content-type"),
                ChecksumAlgorithm="SHA256",
                ChecksumSHA256=base64.b64encode(img_hash.digest()).decode("utf-8"),
            )

        return f"s3://{S3_BUCKET}/{img_path}", f"sha256:{img_sha}"


def handler(context, event):
    data = json.loads(event.body.decode("utf-8"))

    search_id = data["search_id"]
    content_id = data["id"]
    video_id = data["video_id"]
    dataOwner = "FBK"
    date = datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    context.logger.info(
        "handle thumbnail for video {}:{}-{}".format(search_id, content_id, video_id)
    )

    download_types = THUMB_TYPES.copy()

    # handle embedded when available
    image_url = data.get("image_url", None)
    image_type = data.get("image_type", "snippethq")

    if image_url:
        context.logger.info("Collect thumbnail for " + video_id + " " + image_type)

        try:
            img_path, img_hash = fetch_image(image_url, context.s3c)

            row = {
                "id": str(uuid.uuid4()),
                "search_id": search_id,
                "content_id": content_id,
                "video_id": video_id,
                "data_owner": dataOwner,
                "created_at": date,
                "image_type": image_type,
                "image_url": image_url,
                "image_path": img_path,
                "image_hash": img_hash,
            }

            m = json.loads(json.dumps(row))
            k = m["search_id"] + "|" + m["content_id"] + "|" + m["id"]
            context.producer.send("youtube.video_thumbnails", key=k, value=m)

        except Exception as e:
            context.logger.error(f"Error with download: {e}")

    else:
        # add default to list if missing when we do not have a selected thumbnail
        if not "default" in download_types:
            download_types.append("default")

    # additional download when defined
    for image_type in download_types:
        context.logger.info(
            "Collect additional thumbnail for " + video_id + " " + image_type
        )

        try:
            image_url = f"https://img.youtube.com/vi/{video_id}/{image_type}.jpg"
            img_path, img_hash = fetch_image(image_url, context.s3c)

            row = {
                "id": str(uuid.uuid4()),
                "search_id": search_id,
                "content_id": content_id,
                "video_id": video_id,
                "data_owner": dataOwner,
                "created_at": date,
                "image_type": image_type,
                "image_url": image_url,
                "image_path": img_path,
                "image_hash": img_hash,
            }

            m = json.loads(json.dumps(row))
            k = m["search_id"] + "|" + m["content_id"] + "|" + m["id"]
            context.producer.send("youtube.video_thumbnails", key=k, value=m)

        except Exception as e:
            context.logger.error(f"Error with download: {e}")
            continue

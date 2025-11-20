#!/usr/bin/env python3

import os
import csv
import random
import time
import re
from datetime import datetime
from typing import Dict, List

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

API_KEY = os.environ.get("YOUTUBE_API_KEY")
if not API_KEY:
    raise SystemExit("Set YOUTUBE_API_KEY environment variable before running.")

INPUT_FILE = "channels_1000_9999_extendedPartExtra2.csv"   # change if needed
OUTPUT_FILE = "data_1000-9999_Extra2.csv"

YOUTUBE = build("youtube", "v3", developerKey=API_KEY)

MAX_RETRIES = 5
SLEEP_BASE = 1.0
PLAYLISTITEMS_PAGE_SIZE = 50
VIDEO_BATCH_SIZE = 50


def backoff_sleep(attempt: int) -> None:
    time.sleep(SLEEP_BASE * (2 ** attempt))


def safe_channels_list_contentDetails(channel_id: str) -> Dict:
    for attempt in range(MAX_RETRIES):
        try:
            return YOUTUBE.channels().list(
                part="contentDetails",
                id=channel_id,
                maxResults=1
            ).execute()
        except HttpError as e:
            print(f"channels.list error (attempt {attempt}) for {channel_id}: {e}")
            backoff_sleep(attempt)
    raise SystemExit(f"channels.list failed for {channel_id} after retries")


def safe_playlistitems_list(playlist_id: str, page_token: str = None) -> Dict:
    for attempt in range(MAX_RETRIES):
        try:
            return YOUTUBE.playlistItems().list(
                part="snippet",
                playlistId=playlist_id,
                maxResults=PLAYLISTITEMS_PAGE_SIZE,
                pageToken=page_token
            ).execute()
        except HttpError as e:
            print(f"playlistItems.list error (attempt {attempt}) for {playlist_id}: {e}")
            backoff_sleep(attempt)
    raise SystemExit(f"playlistItems.list failed for {playlist_id} after retries")


def safe_videos_list(ids: List[str]) -> Dict:
    for attempt in range(MAX_RETRIES):
        try:
            return YOUTUBE.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(ids),
                maxResults=len(ids)
            ).execute()
        except HttpError as e:
            print(f"videos.list error (attempt {attempt}) for batch size {len(ids)}: {e}")
            backoff_sleep(attempt)
    raise SystemExit("videos.list failed after retries")

def parse_iso8601_duration_to_seconds(duration: str) -> int:
    """
    Converts ISO 8601 YouTube durations like 'PT13M37S' into seconds.
    Supports H, M, S.
    """
    if not duration:
        return 0

    pattern = re.compile(
        r'PT'
        r'(?:(\d+)H)?'
        r'(?:(\d+)M)?'
        r'(?:(\d+)S)?'
    )
    match = pattern.match(duration)
    if not match:
        return 0

    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    return hours * 3600 + minutes * 60 + seconds


def compute_title_features(title: str):
    words = title.split()
    title_word_count = len(words)
    title_exclamation_question_count = title.count("!") + title.count("?")
    how_many_numbers = sum(ch.isdigit() for ch in title)
    words_with_uppercase = sum(1 for w in words if any(ch.isupper() for ch in w))
    return title_word_count, title_exclamation_question_count, how_many_numbers, words_with_uppercase


def compute_description_features(description: str):
    description_word_count = len(description.split())
    number_of_hashtags = description.count("#")
    return description_word_count, number_of_hashtags


def parse_published_day_of_week(published_at: str):
    if not published_at:
        return ""
    try:
        dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        return dt.strftime("%A")
    except Exception:
        return ""

def main():
    if not os.path.exists(INPUT_FILE):
        raise SystemExit(f"Input file not found: {INPUT_FILE}")

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        channels = list(reader)

    print(f"Loaded {len(channels)} channels.")

    original_fields = list(channels[0].keys())
    if "title" in original_fields:
        original_fields[original_fields.index("title")] = "channel_title"

    channel_to_videoid = {}
    missing = 0

    for idx, row in enumerate(channels, start=1):
        cid = row["channelId"]

        ch_resp = safe_channels_list_contentDetails(cid)
        items = ch_resp.get("items", [])
        if not items:
            missing += 1
            print(f"No items for channel {cid}")
            continue

        uploads_id = items[0]["contentDetails"]["relatedPlaylists"].get("uploads")
        if not uploads_id:
            missing += 1
            print(f"No uploads playlist for {cid}")
            continue

        pl_resp = safe_playlistitems_list(uploads_id)
        vids = pl_resp.get("items", [])
        if not vids:
            missing += 1
            print(f"No uploaded videos for {cid}")
            continue

        choice = random.choice(vids)
        vid = choice["snippet"]["resourceId"]["videoId"]
        channel_to_videoid[cid] = vid

        print(f"[{idx}] {cid} -> {vid}")
        time.sleep(0.08)

    print(f"Selected videos for {len(channel_to_videoid)} channels. Missing: {missing}")

    all_vids = list(set(channel_to_videoid.values()))
    video_details = {}

    print(f"Batch fetching details for {len(all_vids)} videos...")
    for i in range(0, len(all_vids), VIDEO_BATCH_SIZE):
        batch = all_vids[i:i + VIDEO_BATCH_SIZE]
        resp = safe_videos_list(batch)
        for v in resp.get("items", []):
            video_details[v["id"]] = v
        time.sleep(0.12)

    video_fields = [
        "videoId",
        "video_title",
        "video_description",
        "title_word_count",
        "title_exclamation_question_count",
        "how_many_numbers_in_title",
        "words_with_uppercase_in_title",
        "description_word_count",
        "number_of_hashtags_in_description",
        "video_duration",           # now an integer in seconds
        "category",
        "day_of_week_uploaded",
        "view_count",
        "like_count",
        "comment_count",
    ]

    output_fields = video_fields + original_fields

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=output_fields)
        writer.writeheader()

        for row in channels:
            cid = row["channelId"]
            vid = channel_to_videoid.get(cid)

            features = {k: "" for k in video_fields}

            if vid and vid in video_details:
                v = video_details[vid]
                snip = v.get("snippet", {})
                stats = v.get("statistics", {})
                details = v.get("contentDetails", {})

                title = snip.get("title", "") or ""
                desc = snip.get("description", "") or ""

                (
                    title_wc,
                    exclam_question,
                    num_digits,
                    upper_words
                ) = compute_title_features(title)

                desc_wc, n_hashtags = compute_description_features(desc)

                duration_iso = details.get("duration", "")
                duration_seconds = parse_iso8601_duration_to_seconds(duration_iso)

                features = {
                    "videoId": vid,
                    "video_title": title,
                    "video_description": desc,
                    "title_word_count": title_wc,
                    "title_exclamation_question_count": exclam_question,
                    "how_many_numbers_in_title": num_digits,
                    "words_with_uppercase_in_title": upper_words,
                    "description_word_count": desc_wc,
                    "number_of_hashtags_in_description": n_hashtags,
                    "video_duration": duration_seconds,
                    "category": snip.get("categoryId", ""),
                    "day_of_week_uploaded": parse_published_day_of_week(snip.get("publishedAt")),
                    "view_count": stats.get("viewCount", ""),
                    "like_count": stats.get("likeCount", ""),
                    "comment_count": stats.get("commentCount", ""),
                }

            # merge
            out = {**features, **row}
            if "title" in out:
                out["channel_title"] = out.pop("title")
            writer.writerow(out)

    print(f"Saved: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

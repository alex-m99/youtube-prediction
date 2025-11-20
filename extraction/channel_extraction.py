import os, time, random, string, csv
from googleapiclient.discovery import build

API_KEY = os.environ.get("YOUTUBE_API_KEY")
if not API_KEY:
    raise SystemExit("Set YOUTUBE_API_KEY environment variable.")

youtube = build("youtube", "v3", developerKey=API_KEY)


def random_prefix(k=3):
    charset = string.ascii_letters + string.digits + "-_"
    return "".join(random.choices(charset, k=k))


def safe_search(q):
    for i in range(5):
        try:
            return youtube.search().list(
                part="snippet",
                type="channel",
                q=q,
                maxResults=50
            ).execute()
        except Exception as e:
            time.sleep(2**i)
    raise RuntimeError("Search failed after retries")


found = {}
attempts = 0
TARGET_COUNT = 5
MAX_ATTEMPTS = 3

while len(found) < TARGET_COUNT and attempts < MAX_ATTEMPTS:
    attempts += 1
    pref = random_prefix(k=3)
    try:
        resp = safe_search(pref)
    except Exception as e:
        print("search failed after retries:", e)
        continue

    ids = [item["snippet"]["channelId"] for item in resp.get("items", [])]
    if not ids:
        continue

    # batch fetch statistics + snippet
    for i in range(0, len(ids), 50):
        batch = ids[i:i + 50]
        chan_resp = youtube.channels().list(
            part="statistics,snippet",
            id=",".join(batch)
        ).execute()

        for ch in chan_resp.get("items", []):
            cid = ch["id"]
            stats = ch.get("statistics", {})
            snippet = ch.get("snippet", {})

            if stats.get("hiddenSubscriberCount"):
                continue

            subs = stats.get("subscriberCount")
            if subs is None:
                continue

            try:
                subs_int = int(subs)
            except ValueError:
                continue

            if 1000 <= subs_int <= 9999:
                if cid not in found:
                    found[cid] = {
                        "subscriberCount": subs_int,
                        "title": snippet.get("title", ""),
                        "channel_video_count": int(stats.get("videoCount", 0)),
                        "channel_view_count": int(stats.get("viewCount", 0)),
                        "uploader_country": snippet.get("country", "")
                    }
                    print(f"Found {len(found)}: {cid} {found[cid]['title']} "
                          f"subs={subs_int} videos={found[cid]['channel_video_count']} "
                          f"views={found[cid]['channel_view_count']} country={found[cid]['uploader_country']}")
    time.sleep(0.1)

print("Done. Collected:", len(found))

filename = "./1000_9999/channels_1000_9999_extendedPartExtra2.csv"
with open(filename, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=[
        "channelId", "title", "subscriberCount",
        "channel_video_count", "channel_view_count", "uploader_country"
    ])
    writer.writeheader()
    for channel_id, meta in found.items():
        writer.writerow({
            "channelId": channel_id,
            "title": meta.get("title", ""),
            "subscriberCount": meta.get("subscriberCount", ""),
            "channel_video_count": meta.get("channel_video_count", ""),
            "channel_view_count": meta.get("channel_view_count", ""),
            "uploader_country": meta.get("uploader_country", "")
        })

print(f"Saved {len(found)} channels to {filename}")

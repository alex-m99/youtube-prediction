import os
from googleapiclient.discovery import build


# get youtube api key from environment variable (for windows)
api_key = os.environ.get("YOUTUBE_API_KEY")

youtube = build('youtube', 'v3', developerKey=api_key)

request = youtube.channels().list(
    part='statistics',
    forUsername='GoogleDevelopers',
)

response = request.execute()

print(response)
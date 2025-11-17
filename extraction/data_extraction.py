import os

# get youtube api key from environment variable (for windows)
api_key = os.environ.get("YOUTUBE_API_KEY")

print(f"API Key: {api_key}")
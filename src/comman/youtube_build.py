from googleapiclient.discovery import build
from src.comman.exceptions import ApiKeyNotFoundException
import os
from dotenv import load_dotenv
load_dotenv()

def get_youtube_build():
    if os.environ['YOUTUBE_API_KEY'] is None or os.environ['YOUTUBE_API_KEY'] == '':
        raise ApiKeyNotFoundException
    youtube = build('youtube', 'v3', developerKey=os.getenv('YOUTUBE_API_KEY'))
    return youtube

from traceback import print_tb
from datetime import datetime, timedelta
from dateutil.relativedelta import *
from src.comman.youtube_build import get_youtube_build
from src.comman.file_handling import write_json, read_json
import os
from dotenv import load_dotenv
load_dotenv()
curr_date = datetime.now().astimezone()
month_12_before_date = curr_date - relativedelta(months=+12)
def get_channel_details(channel_id=None, channel_handle=None):
    youtube = get_youtube_build()
    if channel_id:
        request = youtube.channels().list(
            part='brandingSettings,contentDetails,contentOwnerDetails,id,statistics,status,topicDetails', id=channel_id)
        response = request.execute()
        file_path = f'channel_id/channel_id={channel_id}'
    elif channel_handle:
        request = youtube.channels().list(
            part='brandingSettings,contentDetails,contentOwnerDetails,id,statistics,status,topicDetails', forHandle=channel_handle)
        response = request.execute()
        channel_id = response['items'][0]['id']
        file_path = f'channel_id/channel_id={channel_id}'
    else:
        raise Exception('at least pass one of channel_id or channel_handle')
    output_dest = f"{os.getenv('BASE_INPUT_PATH')}/{file_path}.json"
    write_json(response,output_dest)
    return channel_id

def get_video_id_by_channel(channel_id):
    youtube = get_youtube_build()
    if channel_id:
        request = youtube.search().list(part='id', type='video',channelId = channel_id, maxResults=50, order='viewCount')
        file_path = f'videos/video_ids/channel_id={channel_id}'
    else:
        raise Exception('at least pass one of channel_id')
    response = request.execute()
    output_dest = f"{os.getenv('BASE_INPUT_PATH')}/{file_path}.json"
    write_json(response,output_dest)
    video_ids = ''
    for i in response['items']:
        if i == response['items'][len(response['items'])-1]:
            video_ids = video_ids + f"{i['id']['videoId']}"
        else:
            video_ids = video_ids + f"{i['id']['videoId']},"
    get_video_details(video_ids, channel_id=channel_id)
    return response

def get_video_id_by_channel_full_refresh(channel_id,nextPageToken=None, full_refresh = False):
    youtube = get_youtube_build()
    if channel_id:
        if nextPageToken:
            request = youtube.search().list(part='id', type='video',channelId = channel_id, maxResults=50,
                                            order='viewCount', publishedBefore = curr_date.isoformat(),
                                            publishedAfter = month_12_before_date.isoformat(), pageToken = nextPageToken)
            response = request.execute()
            file_path = f'videos/video_ids/channel_id={channel_id}_nextPageToken={nextPageToken}'
        else:
            request = youtube.search().list(part='id', type='video', channelId=channel_id, maxResults=50,
                                            order='viewCount', publishedBefore = curr_date.isoformat(),
                                            publishedAfter = month_12_before_date.isoformat())
            response = request.execute()
            file_path = f'videos/video_ids/channel_id={channel_id}'
    else:
        raise Exception('at least pass one of channel_id')
    output_dest = f"{os.getenv('BASE_INPUT_PATH')}/{file_path}.json"
    write_json(response,output_dest)
    video_ids = ''
    for i in response['items']:
        if i == response['items'][len(response['items'])-1]:
            video_ids = video_ids + f"{i['id']['videoId']}"
        else:
            video_ids = video_ids + f"{i['id']['videoId']},"

    get_video_details(video_ids=video_ids, channel_id=channel_id, nextPageToken=nextPageToken)

    if 'nextPageToken' in response.keys() and full_refresh:
        if response['nextPageToken'] and full_refresh:
            get_video_id_by_channel_full_refresh(channel_id, nextPageToken=response['nextPageToken'], full_refresh=True)
    return response

def get_video_details(video_ids, channel_id, nextPageToken=None):
    youtube = get_youtube_build()
    if nextPageToken:
        file_path = f'videos/video_details/channel_id={channel_id}_nextPageToken={nextPageToken}'
    else:
        file_path = f'videos/video_details/channel_id={channel_id}'
    request = youtube.videos().list(part='id,statistics,contentDetails,liveStreamingDetails,topicDetails', id=video_ids)
    response = request.execute()
    output_dest = f"{os.getenv('BASE_INPUT_PATH')}/{file_path}.json"
    write_json(response,output_dest)
    return response




def search_videos(query):
    youtube = get_youtube_build()
    request = youtube.search().list(part='id', type='video', q=query, maxResults=5)
    response = request.execute()
    return response

def get_playlist_details(playlist_id):
    youtube = get_youtube_build()
    request = youtube.playlists().list(part='snippet', id=playlist_id)
    response = request.execute()
    return response


from dataclasses import dataclass
from http.client import responses

from pyasn1.type.univ import Boolean

from src.comman.youtube_build import get_youtube_build
from src.comman.file_handling import write_json, read_json
from src.comman.exceptions import exception_alert
import os
from dotenv import load_dotenv
load_dotenv()

@dataclass
class YouTubeDataExtractor:
    
    def get_channel_details(self, channel_id=None, channel_handle=None):
        skip_flag = None
        try:
            skip_flag = False
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
        except Exception as E:
            skip_flag = True
            exception_alert(channel_id, f'get_channel_details', E)
        finally:
            print(f'for {channel_id} get_channel_details function completed')
            return channel_id, skip_flag
    
    def get_video_id_by_channel(self,channel_id):
        response, skip_flag = None, None
        try:
            skip_flag = False
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
            self.get_video_details(video_ids, channel_id=channel_id)
        except Exception as E:
            skip_flag = True
            exception_alert(channel_id, f'get_video_id_by_channel', E)
        finally:
            print(f'for {channel_id} get_video_id_by_channel function completed')
            return response, skip_flag

    
    def get_video_details(self, video_ids, channel_id):
        try:
            youtube = get_youtube_build()
            file_path = f'videos/video_details/channel_id={channel_id}'
            request = youtube.videos().list(part='id,statistics,contentDetails,liveStreamingDetails,topicDetails', id=video_ids)
            response = request.execute()
            output_dest = f"{os.getenv('BASE_INPUT_PATH')}/{file_path}.json"
            write_json(response,output_dest)
            return response
        except Exception as E:
            exception_alert(channel_id, f'get_video_details', E)
    
    def search_videos(self, query):
        try:
            youtube = get_youtube_build()
            request = youtube.search().list(part='id', type='video', q=query, maxResults=5)
            response = request.execute()
            return response
        except Exception as E:
            exception_alert(query, f'search_videos', E)
    
    def get_playlist_details(self, playlist_id):
        youtube = get_youtube_build()
        request = youtube.playlists().list(part='snippet', id=playlist_id)
        response = request.execute()
        return response


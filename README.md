## YouTube_analysis
#### Build a scalable YouTube data pipeline to capture and analyze video metrics (e.g., number of videos, likes, comments, subscribers, playlists).
## Technology used
- Python
- Pyspark
- googleapiclient
- AWS
## ENV
- YOUTUBE_API_KEY = 'YOUTUBE_API_KEY'
- BASE_INPUT_PATH = 'RAW DATA WILL BE SAVED AT THIS LOCATION' 
- BASE_OUTPUT_PATH = 'PROCESSED OR INTERMEDIATE DATA WILL BE SAVED AT THIS LOCATION'
## Setup
- create virtual env `python3 venv -m venv`
- install all dependencies `pip3 install -r requirement.txt`
## To Run
- `python3 workflow.py`
## Function walk through
### get_channel_details
- Based on channel_id or channel_handle, fetches all details related to channel
- It's fetching these details`brandingSettings, contentDetails, contentOwnerDetails, id, statistics, status, topicDetails'`
- saves it into json file to desired location(based on env`BASE_INPUT_PATH`)
### get_video_id_by_channel
- Based on channel_id, fetches top 50 videos id's (order by `viewCount`)
- saves video ids into json file to desired location(based on env`BASE_INPUT_PATH`)
- After getting all videos ids, using video_ids fetches all video details
- saves video details into json file to desired location(based on env`BASE_INPUT_PATH`)
### channel_detail_processing
- Based on channel_id, process all details related to channel
- save channel details into csv file to desired location(based on env`BASE_OUTPUT_PATH`) 
### video_details_processing
- Based on channel_id, process all details related to video for that channel_id
- save video details into csv file to desired location(based on env`BASE_OUTPUT_PATH`)

## Proposed AWS Architecture
![Alt text](https://github.com/devendra631997/YouTube_analysis/blob/main/docs/aws.jpg "Title")
## YouTube Analysis
#### Build a scalable YouTube data pipeline to capture and analyze video metrics (e.g., number of videos, likes, comments, subscribers, playlists).
## Technology used
- Python [ref](https://www.python.org)
- Pyspark [ref](https://spark.apache.org/docs/latest/api/python/index.html)
- googleapiclient [ref](https://developers.google.com/android/reference/com/google/android/gms/common/api/GoogleApiClient)
- AWS [ref](https://aws.amazon.com)
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
### [get_channel_details](https://github.com/devendra631997/YouTube_analysis/blob/458be49070ccf73b380476be3995c5010a74b17c/src/extractor/details.py#L7)
- Based on channel_id or channel_handle, fetches all details related to channel
- It's fetching these details`brandingSettings, contentDetails, contentOwnerDetails, id, statistics, status, topicDetails'`
- saves it into json file to desired location(based on env`BASE_INPUT_PATH`)
### [get_video_id_by_channel](https://github.com/devendra631997/YouTube_analysis/blob/458be49070ccf73b380476be3995c5010a74b17c/src/extractor/details.py#L26)
- Based on channel_id, fetches top 50 videos id's (order by `viewCount`)
- saves video ids into json file to desired location(based on env`BASE_INPUT_PATH`)
- After getting all videos ids, using video_ids fetches all video details`'id, statistics, contentDetails, liveStreamingDetails, topicDetails'` using [get_video_details](https://github.com/devendra631997/YouTube_analysis/blob/458be49070ccf73b380476be3995c5010a74b17c/src/extractor/details.py#L45C5-L45C22) 
- saves video details into json file to desired location(based on env`BASE_INPUT_PATH`)
### [channel_detail_processing](https://github.com/devendra631997/YouTube_analysis/blob/458be49070ccf73b380476be3995c5010a74b17c/src/intermediate/processing.py#L14)
- Based on channel_id, process all details related to channel
- save channel details into csv file to desired location(based on env`BASE_OUTPUT_PATH`) 
### [video_details_processing](https://github.com/devendra631997/YouTube_analysis/blob/458be49070ccf73b380476be3995c5010a74b17c/src/intermediate/processing.py#L34)
- Based on channel_id, process all details related to video for that channel_id
- save video details into csv file to desired location(based on env`BASE_OUTPUT_PATH`)

## Proposed AWS Architecture
![Architecture](https://github.com/devendra631997/YouTube_analysis/blob/main/docs/aws.jpg "Title")

## Anlysis NoteBook
[Analysis](https://github.com/devendra631997/YouTube_analysis/blob/main/view_analysis/analysis.ipynb)

## Presentation Deck
[Presentation](https://docs.google.com/presentation/d/1Yy2llo3GB8m5K982WG-uDMWFGlDwzEugeqgTUbX_0ok/edit?usp=sharing)

## Created By
Devendra Kumar Singh [website](https://devendraksingh.netlify.app)

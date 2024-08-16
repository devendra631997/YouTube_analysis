from debugpy.launcher import channel
from pyarrow.jvm import schema
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,explode
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, BooleanType

import os
from dotenv import load_dotenv

from src.intermediate.schema import channel_detail_schema

load_dotenv()
spark=SparkSession.builder.appName("youTubeAnalysis").getOrCreate()

channel_handles_value = os.getenv('CHANNEL_HANDLES_VALUE').split(',')
channel_ids_value = os.getenv('CHANNEL_IDS_VALUE').split(',')

# Here is Extractor
from extractor.details import get_channel_details, get_video_id_by_channel
# Here is intermediate processing
from intermediate.processing import channel_detail_processing, video_details_processing

def with_channel_handles(channel_handles):
    for channel_handle in channel_handles:
        channel_id= get_channel_details(channel_handle= channel_handle)
        print(channel_id)
        video_details_by_channel = get_video_id_by_channel(channel_id = channel_id)
        print(video_details_by_channel)
        path = channel_detail_processing(channel_id=channel_id)
        print(path)
        path = video_details_processing(channel_id=channel_id)
        print(path)

def with_channel_ids(channel_ids):
    for channel_id in channel_ids:
        channel_id= get_channel_details(channel_id= channel_id)
        print(channel_id)
        video_details_by_channel = get_video_id_by_channel(channel_id = channel_id)
        print(video_details_by_channel)
        path = channel_detail_processing(channel_id=channel_id)
        print(path)
        path = video_details_processing(channel_id=channel_id)
        print(path)

# with_channel_ids(channel_ids_value)
with_channel_handles(channel_handles_value)

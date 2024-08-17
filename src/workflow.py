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
from src.extractor.details import YouTubeDataExtractor
# Here is intermediate processing
from intermediate.processing import YouTubeDataProcessing

def with_channel_handles(channel_handles):
    extractor = YouTubeDataExtractor()
    processor = YouTubeDataProcessing()
    for channel_handle in channel_handles:
        channel_id, skip_flag= extractor.get_channel_details(channel_handle= channel_handle)
        if skip_flag:
            continue
        video_details_by_channel, skip_flag = extractor.get_video_id_by_channel(channel_id = channel_id)
        if skip_flag:
            continue
        path, skip_flag = processor.channel_detail_processing(channel_id=channel_id)
        if skip_flag:
            continue
        path, skip_flag = processor.video_details_processing(channel_id=channel_id)
        if skip_flag:
            continue

def with_channel_ids(channel_ids):
    extractor = YouTubeDataExtractor()
    processor = YouTubeDataProcessing()
    for channel_id in channel_ids:
        channel_id, skip_flag= extractor.get_channel_details(channel_id= channel_id)
        if skip_flag:
            continue
        video_details_by_channel, skip_flag = extractor.get_video_id_by_channel(channel_id = channel_id)
        if skip_flag:
            continue
        path, skip_flag = processor.channel_detail_processing(channel_id=channel_id)
        if skip_flag:
            continue
        path, skip_flag = processor.video_details_processing(channel_id=channel_id)
        if skip_flag:
            continue
#
with_channel_ids(channel_ids_value)
# with_channel_handles(channel_handles_value)

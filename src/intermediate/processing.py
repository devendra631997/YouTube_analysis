from dataclasses import dataclass

from pyarrow.jvm import schema
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,explode
from pyspark.sql.functions import from_json, col, lit
import os
from dotenv import load_dotenv

from src.comman.file_handling import read_json
from src.comman.exceptions import exception_alert

load_dotenv()
spark=SparkSession.builder.appName("youTubeAnalysis").getOrCreate()
from src.intermediate.schema import video_details_schema

@dataclass
class YouTubeDataProcessing:
    @staticmethod
    def channel_detail_processing(channel_id=None, channel_handle=None):
        output_path,skip_flag = None,None
        try:
            if channel_id:
                file_path = f'channel_id/channel_id={channel_id}'
            elif channel_handle:
                file_path = f'channel_handle/channel_handle={channel_handle}'
            else:
                raise Exception('at least pass one of channel_id or channel_handle')
            input_path = f"{os.getenv('BASE_INPUT_PATH')}/{file_path}.json"
            output_path = f"{os.getenv('BASE_OUTPUT_PATH')}/{file_path}.parquet"
            df = spark.read.json(input_path)
            df2 = df.select(col('*'),explode(df.items).alias("item_values") )
            df3 = df2.select(col('*'),'item_values.*' )
            df4 = df3.select(df.etag, df.kind, df3.id,'status.*', 'brandingSettings.channel.title', 'statistics.*')
            df4.toPandas().to_parquet(output_path)
        except Exception as E:
            skip_flag = True
            exception_alert(channel_id, f'channel_detail_processing', E)
        finally:
            print(f'for {channel_id} channel_detail_processing function completed')
            return output_path, skip_flag
    @staticmethod
    def video_details_processing(channel_id):
        output_path, skip_flag = None, None
        try:
            file_path = f'videos/video_details/channel_id={channel_id}'
            input_path = f"{os.getenv('BASE_INPUT_PATH')}/{file_path}.json"
            df = spark.read.json(input_path)
            df2 = df.select(explode(df.items).alias("item_values"))
            df3 = df2.select('item_values.*')
            df4 = df3.select(lit(channel_id).alias('channel_id'),df3.etag, df3.kind,df3.id, 'contentDetails.*', 'statistics.*')
            output_path = f"{os.getenv('BASE_OUTPUT_PATH')}/videos/channel_id={channel_id}.parquet"
            df4.toPandas().to_parquet(output_path)
        except Exception as E:
            skip_flag = True
            exception_alert(channel_id, f'video_details_processing', E)
        finally:
            print(f'for {channel_id} video_details_processing function completed')
            return output_path, skip_flag

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,explode
from pyspark.sql.functions import from_json, col, lit
import os
from dotenv import load_dotenv

from src.comman.file_handling import read_json

load_dotenv()
spark=SparkSession.builder.appName("youTubeAnalysis").getOrCreate()


def channel_detail_processing(channel_id=None, channel_handle=None):
    try:
        if channel_id:
            file_path = f'channel_id/channel_id={channel_id}'
        elif channel_handle:
            file_path = f'channel_handle/channel_handle={channel_handle}'
        else:
            raise Exception('at least pass one of channel_id or channel_handle')
        input_path = f"{os.getenv('BASE_INPUT_PATH')}/{file_path}.json"
        output_path = f"{os.getenv('BASE_OUTPUT_PATH')}/{file_path}.csv"
        df = spark.read.json(input_path)
        df2 = df.select(col('*'),explode(df.items).alias("item_values") )
        df3 = df2.select(col('*'),'item_values.*' )
        df4 = df3.select(df.etag, df.kind, 'pageInfo.*','status.*', 'brandingSettings.channel.*', 'statistics.*')
        df4.toPandas().to_csv(output_path,index_label='record_no')
        # TODO: Channel ID and channel handle is missing
        return output_path
    except Exception as e:
        print(e)


def video_details_processing(channel_id):
    file_path = f'videos/video_details/channel_id={channel_id}'
    input_path = f"{os.getenv('BASE_INPUT_PATH')}/{file_path}.json"
    df = spark.read.json(input_path)
    df2 = df.select(explode(df.items).alias("item_values"))
    df3 = df2.select('item_values.*')
    df4 = df3.select(lit(channel_id).alias('channel_id'),df3.etag, df3.kind,df3.id, 'contentDetails.*', 'statistics.*')
    output_path = f"{os.getenv('BASE_OUTPUT_PATH')}/videos/channel_id={channel_id}.csv"
    df4.toPandas().to_csv(output_path,index_label='record_no')
    return output_path
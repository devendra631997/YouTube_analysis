from pyspark.sql.types import StructType,StructField, StringType, IntegerType, BooleanType

video_details_schema = StructType([
    StructField("channel_id",StringType(),True),
    StructField("etag",StringType(),True),
    StructField("kind",StringType(),True),
    StructField("id", StringType(), True),
    StructField("caption", StringType(), True),
    StructField("definition", StringType(), True),
    StructField("dimension", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("licensedContent", BooleanType(), True),
    StructField("projection", StringType(), True),
    StructField("commentCount", IntegerType(), True),
    StructField("favoriteCount", IntegerType(), True),
    StructField("likeCount", IntegerType(), True),
    StructField("viewCount", IntegerType(), True)
  ])

channel_detail_schema = StructType([
    StructField("etag",StringType(),True),
    StructField("kind",StringType(),True),
    StructField("id", StringType(), True),
    StructField("isLinked", BooleanType(), True),
    StructField("longUploadsStatus", StringType(), True),
    StructField("madeForKids", BooleanType(), True),
    StructField("privacyStatus", StringType(), True),
    StructField("title", StringType(), True),
    StructField("hiddenSubscriberCount", BooleanType(), True),
    StructField("subscriberCount", IntegerType(), True),
    StructField("videoCount", IntegerType(), True),
    StructField("viewCount", IntegerType(), True)
  ])
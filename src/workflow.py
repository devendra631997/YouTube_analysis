# channel_id = 'UC4p_I9eiRewn2KoU-nawrDg'
# channel_handle = 'straitstimesonline'

channel_handles = ['straitstimesonline','TheBusinessTimes','zaobaodotsg','Tamil_Murasu','BeritaHarianSG1957']

# Here is Extractor
from extractor.details import get_channel_details, get_video_id_by_channel
# Here is intermediate processing
from intermediate.processing import channel_detail_processing, video_details_processing


for channel_handle in channel_handles:
    channel_id= get_channel_details(channel_handle= channel_handle)
    print(channel_id)
    video_details_by_channel = get_video_id_by_channel(channel_id = channel_id)
    print(video_details_by_channel)
    path = channel_detail_processing(channel_id=channel_id)
    print(path)
    path = video_details_processing(channel_id=channel_id)
    print(path)
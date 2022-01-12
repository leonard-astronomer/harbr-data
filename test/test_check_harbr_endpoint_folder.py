import boto3
from harbr_utilities import tracking
from harbr_utilities import check_harbr_endpoint_folder


session = boto3.Session(profile_name='default')
s3 = session.client('s3')
bucket = "walstad-astronomer"
endpoint = "Wh8CGNSjQeWBbC0Q1j8XEA"
folder = "singleload"

track = tracking("Check for singleload folder.")
contents = check_harbr_endpoint_folder(s3, bucket, endpoint, folder, track)
if not track.success:
    print("Failed: \n{}")
print("{}".format(track.get_summary()))

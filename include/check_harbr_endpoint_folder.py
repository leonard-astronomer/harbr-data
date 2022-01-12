def check_harbr_endpoint_folder(s3, bucket, endpoint, folder, track) -> object:
    folder = endpoint + "/" + folder + "/"
    return list_key_prefix(s3, bucket, folder, track)


def list_key_prefix(s3, bucket, key, track):
    try:
        key_prefix_list = s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=10)['Contents']
    except BaseException as err:
        msg = f"Unknown error: {err=}, {type(err)=}"
        track.failed(msg)
        key_prefix_list = None
    else:
        track.succeeded()

    return key_prefix_list


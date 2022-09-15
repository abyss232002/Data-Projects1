from google.cloud import storage


def list_my_blobs(bucket_name, folder_name, file_pattern):
    """Lists all the blobs in the bucket."""
    # bucket_name = "your-bucket-name"
    file_list = []
    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    # blobs = storage_client.list_blobs(bucket_name, prefix=folder_name + '/' + file_pattern)
    blobs = storage_client.list_blobs(bucket_name, prefix=folder_name)

    for file in blobs:
        if '.' in file.name and file_pattern in file.name:
            file_list.append(file.name)
            print(file.name)

    return file_list


def move_blob(origin_bucket, origin_blob, destination_bucket, destination_blob):
    """Copies a blob from one bucket to another with a new name."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"
    # destination_bucket_name = "destination-bucket-name"
    # destination_blob_name = "destination-object-name"

    storage_client = storage.Client()

    s_bucket = storage_client.bucket(origin_bucket)
    s_blob = s_bucket.blob(origin_blob)
    d_bucket = storage_client.bucket(destination_bucket)

    blob_copy = s_bucket.copy_blob(s_blob, d_bucket, destination_blob)

    # # Uncomment this line is used if we want to delete the files after copying.
    # s_bucket.delete_blob(origin_blob)

    print("Blob {} in bucket {} moved to blob {} in bucket {}.".format(
            s_blob.name, s_bucket.name, blob_copy.name, d_bucket.name))


def execute_move_files(**kwargs):
    source_bucket = kwargs.get('source_bucket')
    source_folder = kwargs.get('source_folder')
    target_bucket = kwargs.get('target_bucket')
    target_folder = kwargs.get('target_folder')
    my_file_pattern = kwargs.get('file_pattern')

    x = list_my_blobs(source_bucket, source_folder, my_file_pattern)
    for blob in x:
        # # Time Stamp of all files should be of format '_20201126191933676728_28995.txt'.
        # date_folder = blob[-30:-26] + '-' + blob[-26:-24] + '-' + blob[-24:-22]
        # hour_folder = blob[-22:-20]
        # new_blob_name = blob[len(source_folder)+1:]
        # target_blob = target_folder + '/' + date_folder + '/' + hour_folder + '/' + new_blob_name

        # Else below variables to be modified accordingly
        new_blob_name = blob[len(source_folder) + 1:]
        target_blob = target_folder + '/' + new_blob_name

        print(target_blob)

        move_blob(source_bucket, blob, target_bucket, target_blob)

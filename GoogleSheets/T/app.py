import os
import s3fs

S3_BUCKET = os.environ.get('S3_BUCKET')
S3_STAGING_FOLDER = os.environ.get('S3_STAGING_FOLDER')
S3_PUBLISH_FOLDER = os.environ.get('S3_PUBLISH_FOLDER')
S3_PUBLISH_ARCHIVE_FOLDER = os.environ.get('S3_PUBLISH_ARCHIVE_FOLDER')


# Archive publish content
def archive_content():
    try:
        print("------------------------------------------")
        s3 = s3fs.S3FileSystem(anon=False)
        s3_bucket_publish_path = S3_BUCKET + '/' + S3_PUBLISH_FOLDER + '/'
        s3_bucket_publish_archive_path = S3_BUCKET + '/' + S3_PUBLISH_ARCHIVE_FOLDER + '/'
        print("S3 Publish bucket path -- {0}".format(s3_bucket_publish_path))
        print("S3 Publish archive bucket path -- {0}".format(s3_bucket_publish_archive_path))
        print("------------------------------------------")
        archive_content_list = s3.find(s3_bucket_publish_path)
        archive_count = 0
        for file in archive_content_list:
            if file.endswith(".csv"):
                s3_bucket_publish_archive_path = file.replace(S3_PUBLISH_FOLDER,S3_PUBLISH_ARCHIVE_FOLDER)
                print("------------------------------------------")
                print("FileName - {0} is archiving to {1}".format(file, s3_bucket_publish_archive_path))
                print("------------------------------------------")
                s3.mv(file, s3_bucket_publish_archive_path)
                archive_count = archive_count + 1
        print("------------------------------------------")
        if archive_count > 0:
            print("All files archived...")
        else:
            print("No files to archive...")
        print("------------------------------------------")
    except Exception as e:
        print("error occurred - {}".format(e))


# To copy staging content to publish
def move_content_to_publish():
    try:
        print("------------------------------------------")
        s3 = s3fs.S3FileSystem(anon=False)
        s3_bucket_staging_path = S3_BUCKET + '/' + S3_STAGING_FOLDER + '/'
        s3_bucket_publish_path = S3_BUCKET + '/' + S3_PUBLISH_FOLDER + '/'
        print("S3 Staging bucket path -- {0}".format(s3_bucket_staging_path))
        print("S3 Publish bucket path -- {0}".format(s3_bucket_publish_path))
        print("------------------------------------------")
        archive_content_list = s3.find(s3_bucket_staging_path)
        publish_count = 0
        for file in archive_content_list:
            if file.endswith(".csv"):
                s3_bucket_publish_path = file.replace(S3_STAGING_FOLDER,S3_PUBLISH_FOLDER)
                print("------------------------------------------")
                print("FileName - {0} is moving to publish folder {1}".format(file, s3_bucket_publish_path))
                print("------------------------------------------")
                s3.cp(file, s3_bucket_publish_path)
                publish_count = publish_count + 1
        print("------------------------------------------")
        if publish_count > 0:
            print("All files copied to publish folder...")
        else:
            print("No files to copy...")
        print("------------------------------------------")
    except Exception as e:
        print("error occurred - {}".format(e))


# Handler
def staging_to_publish(events,context):
    time.sleep(120)
    archive_content()
    move_content_to_publish()
    return "Successful"




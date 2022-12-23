import os
from datetime import datetime
from ftplib import FTP
import s3fs
from cryptography import fernet
from cryptography.fernet import Fernet

KEY = os.environ.get('KEY')
FTP_HOST = os.environ.get('FTP_HOST')
FTP_USER = os.environ.get('FTP_USER')
FTP_PASSWORD = os.environ.get('FTP_PASSWORD')
FTP_PATH = os.environ.get('FTP_PATH')
S3_BUCKET = os.environ.get('S3_BUCKET')
S3_STAGING_FOLDER = os.environ.get('S3_STAGING_FOLDER')
S3_ARCHIVE_FOLDER = os.environ.get('S3_ARCHIVE_FOLDER')

fernet = Fernet(KEY.encode('utf-8'))


# Get content to archive
def archive_content():
    try:
        print("------------------------------------------")
        s3 = s3fs.S3FileSystem(anon=False)
        s3_bucket_staging_path = S3_BUCKET + '/' + S3_STAGING_FOLDER + '/'
        s3_bucket_archive_path = S3_BUCKET + '/' + S3_ARCHIVE_FOLDER + '/'
        print("S3 Staging bucket path -- {0}".format(s3_bucket_staging_path))
        print("S3 Archive bucket path -- {0}".format(s3_bucket_archive_path))
        print("------------------------------------------")
        archive_content_list = s3.find(s3_bucket_staging_path)
        for file in archive_content_list:
            if file.endswith(".csv"):
                print("FileName - {0} is archiving to {1}".format(file, s3_bucket_archive_path))
                s3.mv(file, s3_bucket_archive_path)
        print("------------------------------------------")
        print("All files Archived...")
        print("------------------------------------------")
    except Exception as e:
        print("error occurred - {}".format(e))


# Decrypting an encrypted value using a key
def decrypt(encrypted_data):
    decrypted_data = fernet.decrypt(encrypted_data).decode()
    return decrypted_data


# Encrypting a decrypted value using a key
def encrypt(value):
    encrypted_data = fernet.encrypt(value.encode('utf-8'))
    return encrypted_data

# To extract the data from 3cLogic FTP server and to load the data to S3
def extraction_load():
    try:
        ftp = FTP(FTP_HOST, FTP_USER, decrypt(FTP_PASSWORD.encode('utf-8')))
        file_list = ftp.nlst(FTP_PATH)
        s3 = s3fs.S3FileSystem(anon=False)
        # current_timestamp = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
        download_count = 0
        print("------------------------------------------")
        print("FTP to S3 file upload starting.....")
        print("FTP file list -- {0}".format(file_list))
        for file in file_list:
            if file.endswith(".csv"):
                file_timestamp = file.split('-')[-1].split('.')[0]
                file_datetime = datetime.fromtimestamp((float(file_timestamp) / 1000))
                s3_filename = file_datetime.strftime("%m_%d_%Y_%H_%M_%S") + '.csv'
                ftp_file_path = FTP_PATH + '/' + file
                print("------------------------------------------")
                print("FTP file path -- {0}".format(ftp_file_path))
                print("S3 file path -- {0}/{1}/{2}".format(S3_BUCKET, S3_STAGING_FOLDER, s3_filename))
                print("Uploading to S3...............")
                ftp.retrbinary('RETR ' + FTP_PATH + '/' + file,
                               s3.open("{}/{}/{}".format(S3_BUCKET, S3_STAGING_FOLDER, s3_filename), 'wb').write)
                download_count = download_count + 1
                print("------------------------------------------")
        print("{0} files successfully uploaded to S3.....".format(download_count))
        print("..... Uploading complete .....")
        print("------------------------------------------")
    except Exception as e:
        print("error occurred - {}".format(e))


#handler
def extract_load_3clogic(events,context):
    archive_content()
    extraction_load()
    print("-----3CLogic_EL_Ended-----")
    print("Successfull")
    return "Successfull"
    

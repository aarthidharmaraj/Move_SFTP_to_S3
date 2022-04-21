"""This module has S3 folder details and actions on that folder"""
import configparser
import os
import logging
import shutil

config = configparser.ConfigParser()
config.read("details.ini")

logging.basicConfig(
    filename="sftptos3_logfile.log", format="%(asctime)s %(message)s", filemode="w"
)
logger = logging.getLogger()


class S3details:
    """This class has the methods for s3 service"""

    def __init__(self):
        """This is the init method of the class S3Service"""
        # self.sftp_path = config["SFTP"]["sftp_path"]
        self.s3_path = config["s3"]["bucket_path"]

    def upload_file(self, filename, path, sftp_source):
        """This method gets file from sftp,uploads into s3 bucket in the given path"""
        new_dir = self.s3_path + path
        os.makedirs(new_dir)
        print(new_dir)
        print(sftp_source)
        shutil.copyfile(sftp_source, new_dir)
        logger.info("The file has been uploaded to s3")

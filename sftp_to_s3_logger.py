"""This module has a script for moving files from sftp to s3 in the partition folder"""
import configparser
import re
from datetime import datetime
import logging
import pysftp
import boto3

config = configparser.ConfigParser()
config.read("details.ini")

logging.basicConfig(
    filename="sftptos3_logfile.log", format="%(asctime)s %(message)s", filemode="w"
)
logger = logging.getLogger()


class SftpConnection:
    """This class that contains methods for get file from sftp and renaming after uplode to s3"""

    def __init__(self):
        """This is the init method of the class of SftpCon"""
        self.conn = pysftp.Connection(
            host=config["SFTP"]["host"],
            username=config["SFTP"]["username"],
            password=config["SFTP"]["password"],
        )
        self.s3_client = S3details()
        self.sftp_path = config["SFTP"]["sftp_path"]
        # self.local_path = config["Local"]["local_path"]

    def list_files(self):
        """This method gets the list of files names for the given sftp path by filtering"""
        sftp_file_list = [
            file
            for file in self.conn.listdir(self.sftp_path)
            if not file.startswith("prcssd.")
        ]
        return sftp_file_list

    def get_file(self, file, path):
        """This method get file from sftp without downloading to local file"""
        try:
            with self.conn.open(self.sftp_path + file, "r") as file_name:
                file_name.prefetch()
                logger.info(
                    "The file has been fetched from sftp and passed upload method in s3"
                )
                self.s3_client.upload_file(file_name, path)
        except IOError as Ie:
            print("The file cannot be opened in sftp", Ie)
            logger.error("The file cannot be opened in sftp")

    def rename_file(self, file):
        """This method is used for rename the sftp file or directory after proceesed"""
        new_name = self.sftp_path + "prcssd." + file
        self.conn.rename(self.sftp_path + file, new_name)
        logger.info("The file in sftp has been processed and renamed successfully")
        return new_name


class S3details:
    """This class has the methods for s3 service"""

    def __init__(self):
        """This is the init method of the class S3Service"""
        self.client = boto3.client(
            "s3",
            aws_access_key_id=config["s3"]["aws_access_key_id"],
            aws_secret_access_key=config["s3"]["aws_secret_access_key"],
        )
        self.bucket_name = config["s3"]["bucket"]
        self.bucket_path = config["s3"]["bucket_path"]
        # self.local_path = config["Local"]["local_path"]
        self.sftp_path = config["SFTP"]["sftp_path"]

    def upload_file(self, file, path):
        """This method gets file from sftp,uploads into s3 bucket in the given path"""
        self.client.put_object(
            Bucket=self.bucket_name, Body=file, Key=self.bucket_path + path
        )
        logger.info("The file has been uploaded to s3")
        return self.bucket_path + path


class MoveFileSftpToS3:
    """This is the class for moving file sftp to s3"""

    def __init__(self):
        """This is the init method for the class of MoveFileSftpToS3"""
        self.sftp_conn = SftpConnection()
        self.s3_client = S3details()

    def move_file_to_s3(self):
        """This method moves the file from sftp to s3"""
        sftp_file_list = self.sftp_conn.list_files()
        for file_name in sftp_file_list:
            self.put_path_partition(file_name)

    def put_path_partition(self, file_name):
        """This method uses partioning of path and upload the file to S3"""
        try:
            find_file = re.search("[0-9]{1,2}.[0-9]{1,2}.[0-9]{1,4}.(csv)", file_name)
            upload_date_object = datetime.strptime(find_file, "%d.%m.%y")
            partition_path = (
                "pt_year="
                + upload_date_object.strftime("%Y")
                + "/pt_day="
                + upload_date_object.strftime("%d")
                + "/pt_month="
                + upload_date_object.strftime("%m")
                + "/"
                + file_name
            )
            self.sftp_conn.get_file(file_name, partition_path)
            rename = self.sftp_conn.rename_file(file_name)
            print("The file has been uploaded to s3 and rename in sftp path", rename)
        except Exception as err:
            print("Cannot be uploaded in S3 in the parttioned path", err)
            logger.error("The file cannot be uploaded in the given path in s3")


def main():
    """This is the main method for the module name move_file_sftp_to_s3"""
    move_sftp_to_s3 = MoveFileSftpToS3()
    move_sftp_to_s3.move_file_to_s3()


if __name__ == "__main__":
    main()

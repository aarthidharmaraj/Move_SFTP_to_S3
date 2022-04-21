"""This module has the script for moving files from sftp folder to s3 folder"""
import configparser
import re
from datetime import datetime
import logging
from s3details import S3details
from SFTP_Connection import SftpConnection

config = configparser.ConfigParser()
config.read("details.ini")

logging.basicConfig(
    filename="sftptos3_logfile.log", format="%(asctime)s %(message)s", filemode="w"
)
logger = logging.getLogger()


class MoveSftpToS3:
    """This is the class for moving file sftp to s3"""

    def __init__(self):
        """This is the init method for the class of MoveFileSftpToS3"""
        self.sftp_conn = SftpConnection()
        self.s3_client = S3details()
        self.sftp_path = config["SFTP"]["sftp_path"]

    def move_file_to_s3(self):
        """This method moves the file from sftp to s3"""
        sftp_file_list = self.sftp_conn.list_files()
        for file_name in sftp_file_list:
            copy_source = self.sftp_path + file_name
            self.put_path_partition(file_name, copy_source)

    def put_path_partition(self, file_name, sftp_source):
        """This method uses partioning of path and upload the file to S3"""
        try:
            if re.search("[0-9]{1,2}.[0-9]{1,2}.[0-9]{1,4}.(csv)", file_name):

                # print(splits)
                date_object = datetime.strptime(file_name, "%d.%m.%Y.csv")
                print(date_object)
                partition_path = (
                    "pt_year="
                    + date_object.strftime("%Y")
                    + "/pt_month="
                    + date_object.strftime("%m")
                    + "/pt_day="
                    + date_object.strftime("%d")
                )
                print(partition_path)
                self.s3_client.upload_file(file_name, partition_path, sftp_source)
                rename = self.sftp_conn.rename_file(file_name)
                print("The file has been uploaded to s3 and rename in sftp path")
            else:
                print("The", file_name, "is not in the prescribed format")
        except Exception as err:
            print("Cannot be uploaded in S3 in the parttioned path", err)
            logger.error("The file cannot be uploaded in the given path in s3")


def main():
    """This is the main method for the module name move_file_sftp_to_s3"""
    move_sftp_to_s3 = MoveSftpToS3()
    move_sftp_to_s3.move_file_to_s3()


if __name__ == "__main__":
    main()

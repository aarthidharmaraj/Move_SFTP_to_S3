"""This module has script for the performance on sftp folder"""
import configparser
import logging
import os
from s3details import S3details

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
        self.s3_client = S3details()
        self.sftp_path = config["SFTP"]["sftp_path"]

    def list_files(self):
        """This method gets the list of files names for the given sftp path by filtering"""
        sftp_file_list = [
            files
            for files in os.listdir("MSG SFTP Server/D/boston_frontgate/")
            if not files.startswith("prcssd.")
        ]

        return sftp_file_list

    def rename_file(self, file):
        """This method is used for rename the sftp file or directory after proceesed"""
        new_name = self.sftp_path + "prcssd." + file
        os.rename(self.sftp_path + file, new_name)
        logger.info("The file in sftp has been processed and renamed successfully")
        return new_name

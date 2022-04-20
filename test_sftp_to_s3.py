"""This module tests the python script of moving file from Sftp to S3 with Mocked AWS services """
import os
from unittest import result
import pytest
import boto3
from moto import mock_s3
from mock import *
import pysftp
from sftp_to_s3_test import SftpConnection,S3details,MoveSftpToS3
@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

@pytest.fixture
def mock_sftp(sftp_server):
    """This is the fixture for mocking sftp server"""
    print(sftp_server.host)
    with sftp_server.client("test_user") as client:
        sftp = client.open_sftp()
        yield sftp

@pytest.fixture(scope="function")
def S3(aws_credentials):
    """This method creates a mock for s3 services"""
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")

class Test_s3details:
    """ This Test Class will check for all the possible Test cases in S3details """
    def test_s3_objects(self,S3):
        """ This Method will test for the instance belong to the class S3details """
        self.obj = S3details()
        assert isinstance(self.obj, S3details)
    
    def test_upload_s3_passed(self,S3):
        """ This tests for put object in S3 class S3details"""
        self.s3= S3details()
        res=self.s3.upload_file("flow.txt","flow.txt")
        
        assert res["Contents"][0]["Key"] == "flow.txt"

    @pytest.mark.xfail
    def test_upload_s3_failed(self):
        """ This method will test for the report is uploaded to S3 in class S3Operations """
        self.s3= S3details()
        res=self.s3.upload_file("flow.txt","flow.txt")
        
        assert res["Contents"][0]["Key"] == "flw.txt"
        
class Test_SftpConnection:
        
    def test_sftp_connection(self):
        '''This method tests for the patch mock connection of sftp'''
        with patch("pysftp.Connection") as mock_connection:
            with pysftp.Connection("1.2.3.4", "user", "pwd", 12345) as sftp:
                sftp.get("filename")
            mock_connection.assert_called_with("1.2.3.4", "user", "pwd", 12345)
            sftp.get.assert_called_with("filename")
    
    def test_list_files_passed(self,mock_sftp):
        """This tests for files returning from list_files method in sftp_to_s3 """
        self.sftp= SftpConnection()
        file_list= self.sftp.list_files()
        assert type(file_list) == list

    @pytest.mark.xfail
    def test_list_files_failed(self):
        """This tests for files not returning from list_files method in sftp_to_s3 """
        self.sftp= SftpConnection()
        result = self.sftp.list_files()
        assert result == None
        
class Test_moveSftp_to_s3:
    def test_move_file_sftp_to_s3_objects(self):
        """This tests for the instance belong to the class MoveSftpToS3"""
        self.obj = MoveSftpToS3()
        assert isinstance(self.obj, MoveSftpToS3)
    
    def test_move_file_is_completed(self):
        """This method will test weather the files moved sucessfully"""
        self.obj = MoveSftpToS3()
        result = self.obj.move_file_to_s3()
        assert result == "Success"

    @pytest.mark.xfail
    def test_move_file_failed(self):
        """This method will test weather move file failed"""
        self.obj = MoveSftpToS3()
        result = self.obj.move_file_to_s3()
        assert result == "Failure"
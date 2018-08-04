from __future__ import print_function
import logging
import boto3
import botocore


class S3Handler:

    def __init__(self, bn, r='us-east-1', p='default', prefix=''):
        '''
        Class to handle s3 interaction
        bn = bucket name
        r = region
        p = profile to pull for .aws config deployed by ansibile vault
        '''
        # TODO add boto profiles support for new creds method
        logging.getLogger('boto3').setLevel(logging.WARN)
        logging.getLogger('botocore').setLevel(logging.WARN)
        self.bucket = bn
        self.prefix = prefix
        self.log = logging.getLogger("schema.s3")
        self.profile = p
        self.region = r

    def connect(self):
        try:
            session = boto3.session.Session(profile_name=self.profile)
            self.s3 = session.client('s3', region_name=self.region)
            self.log.debug(self.s3.head_bucket(Bucket=self.bucket))
            self.log.info(
                "Connected to bucket s3://%s using profile: %s",
                self.bucket,
                self.profile)
            return True
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            self.log.error(error_code)
            if error_code == 404:
                self.log.error("Bucket doesn't exist")
                return False
            elif error_code == 403:
                # It is possible to not have read access and only have access
                # to a specific key (dir) in the bucket denoted by the prefix
                if self.prefix != '':
                    try:
                        r = self.s3.head_object(
                            Bucket=self.bucket, Key=self.prefix.lstrip('/'))
                        if r['ResponseMetadata']['HTTPStatusCode'] == 200:
                            self.log.info(
                                "Connected to bucket s3://%s \
                                using profile: %s on key %s",
                                self.bucket,
                                self.profile,
                                self.prefix)
                            return True
                    except botocore.exceptions.ClientError as e:
                        self.log.error(
                            "Access Denied, for head object on \
                            bucket %s and prefix %s",
                            self.bucket,
                            self.prefix.lstrip('/'))
                        return False
                self.log.error("Access Denied, check credentials")
                return False
            else:
                self.log.error("Error code: %s", error_code)
                return False
        except Exception as e:
            self.log.error("Generic exception: %s", e)
            # logging.error("Failed to instantiate s3 client")
            return False

    def lsBucket(self):
        files = {}
        paginator = self.s3.get_paginator('list_objects')
        for i in paginator.paginate(Bucket=self.bucket):
            for j in i.get('Contents'):
                files[j.get('Key')] = j.get('ETag').strip('"')
        return files

    def uploadFile(self, k, fh):
        try:
            fp = open(fh, 'rb')
            k = k.lstrip("/")
            self.s3.put_object(Key=k, Bucket=self.bucket, Body=fp)
            self.log.info("key: %s uploaded to %s", k, self.bucket)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 403:
                self.log.error("Access Denied, failed to upload file")
            else:
                self.log.error("Error code: %s", error_code)
        except Exception as e:
            self.log.error("Generic exception: %s", e)
            # logging.error("Failed to instantiate s3 client")
        return

    def uploadString(self, k, s):
        try:
            k = k.lstrip("/")
            self.s3.put_object(Key=k, Bucket=self.bucket, Body=s)
            self.log.info("key: %s uploaded to %s", k, self.bucket)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 403:
                self.log.error("Access Denied, failed to upload file")
            else:
                self.log.error("Error code: %s", error_code)
        except Exception as e:
            self.log.error("Generic exception: %s", e)
            # logging.error("Failed to instantiate s3 client")
        return

    def getkey(self, k):
        '''
            get key and return file body as a string
            returns None if there is an exception or no data
            k= key to download
        '''
        try:
            self.log.info("key: %s downloaded from %s", k, self.bucket)
            obj = self.s3.get_object(Key=k, Bucket=self.bucket)
            # print obj
            data = obj['Body'].read()
            return data

        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 403:
                self.log.error("Access Denied, failed to download file")
            elif error_code == 404:
                self.log.error("File does not exist, failed to download file")
            else:
                self.log.error("Error code: %s", error_code)
        except Exception as e:
            self.log.error("Generic exception: %s", e)
            # logging.error("Failed to instantiate s3 client")
        return None

    def movefile(self, s, d):
        '''
            move file within a bucket from s to d
        '''
        try:
            self.log.info("%s moved to %s", s, d)
            self.s3.copy_object(Bucket=self.bucket, CopySource="{}/{}".format(self.bucket, s), Key=d)
            self.s3.delete_object(Bucket=self.bucket, Key=s)

        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 403:
                self.log.error("Access Denied, failed to move file")
            elif error_code == 404:
                self.log.error("File does not exist, failed to move file")
            else:
                self.log.error("Error code: %s", error_code)
        except Exception as e:
            self.log.error("Generic exception: %s", e)
            # logging.error("Failed to instantiate s3 client")

    def keyExists(self, k):
        '''
            Check to see if key exists by heading object
        '''
        try:
            self.s3.head_object(Bucket=self.bucket, Key=k)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                return False
        except Exception as e:
            self.log.error("Generic exception: %s", e)
        return True





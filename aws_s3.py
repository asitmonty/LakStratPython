import boto3
import sys
import botocore
import utilities
import os  
from os import listdir
from os.path import isfile, join
class AwsS3:
    global bucket_name
    global bucket

    bucket_name='lakstrat'
    localfile = "D:\\Workspace\\python\\aws\\aws.txt"
    key_vallue = "my_text_file"


    def __init__(self, bucket_name):
        self._s3 = boto3.resource('s3')
        self._bucket = self.get_bucket(bucket_name)


    def get_bucket(self, bucket_name):
        #for bucket in self._s3.buckets.all():
        #  print(bucket.name)
        bucket_exists = self.check_if_bucket_exists(bucket_name)
        if (not bucket_exists):
            self._s3.create_bucket(Bucket=bucket_name)
            #s3.create_bucket(Bucket='montylnkdata/mybucket3')
            #s3.create_bucket(Bucket='mybucket', CreateBucketConfiguration={
            #    'LocationConstraint': 'us-west-1'})
        #Getting a bucket
        bucket = self._s3.Bucket(bucket_name)
        #print all keys in bucket
        # if(exists):
          # for object in bucket.objects.all():
            # print(object.key)
        return bucket


    def check_if_bucket_exists(self, bucket_name):
        exists = True
        try:
            self._s3.meta.client.head_bucket(Bucket=bucket_name)
                #s3.create_bucket(Bucket='mybucket', CreateBucketConfiguration={
        #    'LocationConstraint': 'us-west-1'})
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                exists = False
                #ulog.logit(2, "Failed to get s3 bucket " + bucket_name + ". Error code returned:" + error_code)
        return exists


    def writeDataToS3(self, key, mode, data):
        success = 0
        #set exists True to check if the connection was successful
        #ulog.logit(2, "Connected to s3 bucket: " + bucket_name)
        #self._bucket.upload_file(filename, key)
        object_exists = self.check_if_object_exists(key)
        if(object_exists):
            #ulog.logit(2, "File '" + key + "' uploaded to s3 bucket '" + bucket_name + "'")
            self._bucket.put_object(Key=key, Body=data)
            success = 1
        else:
            self.get_object(key).put(Body=data)
            #ulog.logit(3, "File upload failed. File name:'" + key + "'. Bucket: '" + bucket_name + "'")
            #print "File upload failed. File name:'" + key + "'. Bucket: '" + bucket_name + "'"
            success = 1
        return success

    def writeFileToS3(self, file, key, mode):
        success = 0
        #set exists True to check if the connection was successful
        #ulog.logit(2, "Connected to s3 bucket: " + bucket_name)
        object_exists = self.check_if_object_exists(key)
        if(object_exists):
            #ulog.logit(2, "File '" + key + "' uploaded to s3 bucket '" + bucket_name + "'")
#            self._bucket.put_object(Key=key, Body=data)
            self._bucket.upload_file(file, key)
            success = 1
        else:
#            self.get_object(key).put(Body=data)
            self.get_object(key)
            self._bucket.upload_file(file, key)
            #ulog.logit(3, "File upload failed. File name:'" + key + "'. Bucket: '" + bucket_name + "'")
            #print "File upload failed. File name:'" + key + "'. Bucket: '" + bucket_name + "'"
            success = 1
        return success


    def writeFolderToS3(self, key_path, mode, local_folder_path):

        list_dir = listdir(local_folder_path)
        rowcount = len(list_dir)
        loop_counter = 0
        for fn in list_dir:
            utilities.show_progress(loop_counter, rowcount)  #show progress on screen
            if isfile(join(local_folder_path, fn)):
                a = self.writeFileToS3(local_folder_path + fn, key_path + fn, 'w+')
            loop_counter += 1


    def get_object(self, key):
        object_exists = self.check_if_object_exists(key)
        #Getting a object
        if (not object_exists):
            obj = self._bucket.put_object(Key=key)
        else:
            obj = self._bucket.Object(key)
        return obj


    def check_if_object_exists(self, key):
        exists = True
        try:
            self._bucket.Object(key).load()
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                exists = False
        return exists

    def get_object_data(self, bucket_name, key):
        
        obj = self._s3.Object(bucket_name, key)
        response = obj.get()
        #s3.meta.client.download_file('mybucket', 'hello.txt', '/tmp/hello.txt')
        data = response['Body'].read()
        return data
        #k = Key(bucket_name)
        #k.key = 'key_monty'
        #k.set_contents_from_string('This is a test of S3')
        # key = bucket.get_key(file_key) 
          # if key is None: 
            # key = bucket.new_key(file_key)



def main():
    global bucket_name
    key = 'log.txt'

    s3.Bucket(name='app.monty.lnk.datadump')
    s3.ObjectSummary(bucket_name='montylnkdata', key='alliances.json.gz')
    bucket = get_bucket(s3, bucket_name)
    #s3.Object('mybucket2', 'hello.txt').put(Body=open('/tmp/hello.txt', 'rb'))
    #upload_file(s3, bucket, key, 'log.txt')
    data = get_object_data(s3, bucket_name, key)
    print data


if __name__ == "__main__":
  main()

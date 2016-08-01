import boto3
import sys
import botocore
import utilities
import os  
import time
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
        self._s3client = boto3.client('s3')


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


    def get_object(self, key):
        object_exists = self.check_if_object_exists(key)
        #Getting a object
        if (not object_exists):
            obj = self._bucket.put_object(Key=key)
        else:
            obj = self._bucket.Object(key)
        return obj


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



    def writeFolderToS3_Backup(self, key_path, mode, local_folder_path):
        list_dir = listdir(local_folder_path)
        list_dir.sort()
        rowcount = len(list_dir)
        loop_counter = 0
        start_time = time.time()
        for fn in list_dir:
            end_time = time.time()
            time_elapsed = end_time - start_time
            utilities.show_progress_time(loop_counter, rowcount, time_elapsed)  #show progress on screen
            if isfile(join(local_folder_path, fn)):
                a = self.writeFileToS3_Backup(local_folder_path + fn, key_path + fn, 'w+')
            loop_counter += 1
        utilities.show_progress_time(1, 1, 0)  #show progress on screen



    def writeFileToS3_Backup(self, file, key, mode):
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



    def get_key_list_for_path(self, key_path):
        bucket_exists = self.check_if_bucket_exists(bucket_name)
        if(bucket_exists):
            paginator = self._s3client.get_paginator('list_objects')
            operation_parameters = { 'Bucket': bucket_name, 'Prefix': key_path }
            page_iterator = paginator.paginate( **operation_parameters )
            list_key = []
            if page_iterator:
                for page in page_iterator:
                    if 'Contents' in page:
                        print "contents there"
                        for item in page['Contents']:
                            list_key.append(item['Key'].split('/')[-1])
            return list_key
            #"Contents[?Size > `100`][]"  #filter all key greather than size 100
            '''
            # page['Contents'] gives a json array with each object representing details for each key returned
            #output of following command
                        paginator = self._s3client.get_paginator('list_objects')
                        operation_parameters = {'Bucket': bucket_name,
                                    'Prefix': key_path}
                        page_iterator = paginator.paginate(**operation_parameters)
                        for page in page_iterator:
                            print(page['Contents'])
            [
            {
            u'LastModified': datetime.datetime(2016, 7, 29, 20, 41, 37, tzinfo=tzutc()), 
            u'ETag': '"99914b932bd37a50b983c5e7c90ae93b"', 
            u'StorageClass': 'STANDARD', 
            u'Key': u'2016-07-29/alliances/US11_alliance_0.json', 
            u'Owner': {
                        u'DisplayName': 'asitmonty', 
                        u'ID': '99decaeb60f1e9b5bd3b0fb60a41de8c1318b95a600be8319be19f934b2eb5e0'
                    }, 
            u'Size': 2
            },
            {},{},...]
            '''

    def writeFolderToS3(self, key_path, folderpath, overwrite):

        existing_keys_list = []
        missing_key_list = []
        local_folder_path = folderpath + key_path
        list_dir = listdir(local_folder_path)
        rowcount = len(list_dir)
        start_time = time.time()
        key_list = self.get_key_list_for_path(key_path)
        if key_list:
            existing_keys_list = list(set(list_dir).intersection(key_list))
            existing_keys_list.sort()
            missing_key_list = list(set(list_dir) - set(key_list))
            missing_key_list.sort()
        else:
            missing_key_list = list_dir
            missing_key_list.sort()
        loop_counter = 0
        #iterate first for exisitng keys
        if (overwrite and existing_keys_list):
            for fn in existing_keys_list:
                time_elapsed = time.time() - start_time
                utilities.show_progress_time(loop_counter, rowcount, time_elapsed)  #show progress on screen
                if isfile(join(local_folder_path, fn)):
                    key = key_path + fn
                    file = local_folder_path + fn
                    self._bucket.upload_file(file, key)
                loop_counter += 1
        #iterate next for new keys
        for fn in missing_key_list:
            time_elapsed = time.time() - start_time
            utilities.show_progress_time(loop_counter, rowcount, time_elapsed)  #show progress on screen
            if isfile(join(local_folder_path, fn)):
                key = key_path + fn
                file = local_folder_path + fn
                self.get_object(key)
                self._bucket.upload_file(file, key)
            loop_counter += 1
        utilities.clear_progress_bar



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

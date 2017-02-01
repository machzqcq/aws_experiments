import boto3
import os

session = boto3.session.Session(profile_name='pmacharla-dpnanalytics-dev')
mys3client = session.resource('s3')

for bucket in mys3client.buckets.all():
    print(bucket.name)

# Upload a new file
data = open(os.getcwd()+'/sample_data/blah.txt', 'rb')
mys3client.Bucket('pradeep-temp-test').put_object(Key='blah.txt', Body=data)

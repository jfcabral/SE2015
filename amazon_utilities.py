from sys import stderr

import json
import boto
import boto.sqs
import boto.ec2
import boto.sns
import boto.ses
import boto.emr
import boto.s3
from boto.sqs.message import Message

REGION = 'us-east-1'
INSTANCE_TYPE = 't2.micro'


# Cluster states considered active
ACTIVE_CLUSTER_STATES = ('STARTING', 'BOOTSTRAPPING', 'WAITING', 'RUNNING')
# from http://docs.aws.amazon.com/cli/latest/reference/emr/list-clusters.html
ALL_CLUSTER_STATES = ('STARTING', 'BOOTSTRAPPING', 'WAITING', 'RUNNING',
                      'TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS')
# The only resizable cluster states
RESIZABLE_CLUSTER_STATES = ('WAITING', 'RUNNING')

QUEUE_EMAIL_DISPATCH = 'email_dispatch_queue'


# <----------------------- SC2 ----------------------->
def connect_ec2(region=REGION):
    return boto.ec2.connect_to_region(region)


# <----------------------- Simple Queue Service (SQS) ----------------------->
def connect_sqs(region=REGION):
    return boto.sqs.connect_to_region(region)


# Create and get queue
def create_get_queue(name, conn=connect_sqs()):
    return conn.create_queue(name)


# Sends a message to the Email Notification Queue
def request_job_notification(data):
    queue = create_get_queue(QUEUE_EMAIL_DISPATCH)

    message = Message()
    message.set_body(json.dumps(data))
    queue.write(message)


# <----------------------- S3 (Simple Storage Service) ----------------------->
def connect_s3(region=REGION):
    return boto.s3.connect_to_region(region)


# Buckets
def create_get_bucket(name, conn=connect_s3()):
    conn.create_bucket(name)


# TODO check this
def set_folder_public(bucket_name, file_local_s3_path, conn=connect_s3()):
    bucket = conn.get_bucket(bucket_name)
    # adjust file_local_s3_path to remove s3://<bucket_name/ and get the file key in the s3_name bucket
    _tmp = file_local_s3_path[file_local_s3_path.find('output'):]
    print _tmp
    file_key = bucket.get_key(_tmp)
    if file_key:
        file_key.set_canned_acl('public-read')
    else:
        print >> stderr, 'Error setting folder permissions: ', _tmp


def set_folder_public_simple(file_local_s3_path, conn=connect_s3()):
    _tmp = file_local_s3_path[5:]
    bucket_name = _tmp[:_tmp.find('/')]

    bucket = conn.get_bucket(bucket_name)
    # adjust file_local_s3_path to remove s3://<bucket_name/ and get the file key in the s3_name bucket
    _tmp = file_local_s3_path[file_local_s3_path.find('output'):]
    #print _tmp

    file_key = bucket.get_key(_tmp)
    if file_key:
        file_key.set_canned_acl('public-read')
    else:
        print >> stderr, 'Error setting folder permissions: ', _tmp


# <----------------------- Simple Notification Service (SNS) ----------------------->
def connect_sns(region=REGION):
    return boto.sns.connect_to_region(region)


# <----------------------- Simple Email Service (SES) ----------------------->
def connect_ses(region=REGION):
    return boto.ses.connect_to_region(region)


# <----------------------- Elastic Map Reduce (EMR) ----------------------->
def connect_emr(region=REGION):
    return boto.emr.connect_to_region(region)


# for tests only:
if __name__ == '__main__':
    set_folder_public('eng-serv-teste3', 's3://eng-serv-teste3/output/A2_2015526232334/part-00000')
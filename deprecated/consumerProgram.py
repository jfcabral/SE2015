# from Cloud Computing - A Hands-On Approach

import cPickle
import time

import boto.sqs
from boto.sqs.message import Message
import boto.emr
from boto.emr.step import StreamingStep

from deprecated.emailNotification import sendemail

'''
ACCESS_KEY = "<enter ACCESS_KEY>"
SECRET_KEY = "<enter SECRET_KEY>"
'''
REGION = "eu-west-1"


def sendnotification(msg, status, downloadlink):
    receipients_list = (msg('emailaddress'))
    subject = 'MapReduce Job Notification'

    if status == 'COMPLETED':
        message = "Your MapReduce job is complete. Download results from: " + downloadlink

    sendemail(receipients_list, subject, message)


def createemrjob(msg):
    print "Connecting to EMR"

    conn = boto.emr.connect_to_region(REGION)

    print "Creating streaming step"

    t = time.localtime(time.time())
    job_datetime = str(t.tm_year) + str(t.tm_mon) + str(t.tm_mday) + str(t.tm_hour) + str(t.tm_min) + str(t.tm_sec)

    outputlocation = 's3n://mybucket/uploadfiles/' + job_datetime

    step = StreamingStep(name=job_datetime,
                         mapper=msg('mapper'),
                         reducer=msg('reducer'),
                         input=msg('datafile'),
                         output=outputlocation)

    print "Creating job flow"

    jobid = conn.run_jobflow(name=job_datetime,
                             log_uri='s3n://mybucket/uploadfiles/mapred_logs',
                             steps=[step])

    print "Submitted job flow"

    print "Waiting for job flow to complete"

    status = conn.describe_jobflow(jobid)
    print status.state

    while status.state == 'STARTING' or status.state == 'RUNNING' or status.state == 'WAITING' or status.state == 'SHUTTING_DOWN':
        time.sleep(10)
        status = conn.describe_jobflow(jobid)

    print "Job status: " + str(status.state)

    print "Completed Job: " + job_datetime

    downloadlink = 'http://mybucket.s3.amazonaws.com/uploadedfiles/' + job_datetime + '/part-00000'

    sendnotification(msg, status.state, downloadlink)

    print "Connecting to SQS"

    conn = boto.sqs.connect_to_region(REGION)

    queue_name = 'arsh_queue'

    print "Connecting to queue: " + queue_name
    q = conn.get_all_queues(prefix=queue_name)

    count = q(0).count()

    print "Total messages in queue: " + str(count)

    print "Reading message from queue"

    for i in range(count):
        m = q(0).read()
        msg = cPickle.loads(m.get_body())
        print "Message %d: %s" % (i + 1, msg)
        q(0).delete_message(m)
        createemrjob(msg)

    print "Read %d messages from queue" % count







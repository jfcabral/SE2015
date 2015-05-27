# from Cloud Computing - A Hands-On Approach

import cPickle
import boto.sqs
from boto.sqs.message import Message
import s3upload

'''
ACCESS_KEY = "<enter ACCESS_KEY>"
SECRET_KEY = "<enter SECRET_KEY>"
'''
REGION = "eu-weast-1"


def enqueuejob(datafile, mapper, reducer, emailaddress):
    conn = boto.sqs.connect_to_region(REGION)

    queue_name = 'arsh_queue'

    q = conn.get_all_queues(prefix=queue_name)

    msgdict = {'datafile': datafile,
               'mapper': mapper,
               'reducer': reducer,
               'emailaddress': emailaddress}

    msg = cPickle.dumps(msgdict)

    m = Message()
    m.set_body(msg)
    status = q(0).write(m)


def createjob(datafilename, mapfilename, reducefilename, mapreduceprogram, emailaddress):
    s3upload.upload('mybucket', 'uploadfiles', datafilename)
    datafile = 's3n://mybucket/uploadedfiles/' + datafilename

    if mapreduceprogram == 'wordcount':
        mapper = 's3n://mybucket/uploadedfiles/wordCountMapper.py'
        reducer = 's3n://mybucket/uploadedfiles/wordCountReducer.py'
    elif mapreduceprogram == 'invertedindex':
        mapper = 's3n://mybucket/uploadedfiles/invertedindexMapper.py'
        reducer = 's3n://mybucket/uploadedfiles/invertedindexReducer.py'
    else:
        s3upload.upload('mybucket', 'uploadedfiles', mapfilename)
        s3upload.upload('mybucket', 'uploadedfiles', reducefilename)
        mapper = 's3n://mybucket/uploadedfiles/' + mapfilename
        reducer = 's3n://mybucket/uploadedfiles' + reducefilename

    enqueuejob(datafile, mapper, reducer, emailaddress)
    return datafile, mapper, reducer, emailaddress


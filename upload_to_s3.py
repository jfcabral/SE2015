from boto.s3.connection import S3Connection
from boto.s3.key import Key
from time import time
from time import localtime
import glob
from os import remove


# upload every files inside local input folder. (Delete them (locally) after the upload - not at the moment)
def upload_input_data(s3_name, conn=S3Connection()):

    print
    raw_input("Press enter to start the flow...(do not forget to populate the local input folder)\n")

    # print "Connecting to S3 (Bucket: %s)..." % s3_name
    # conn = S3Connection()
    bucket = conn.get_bucket(s3_name)

    print
    raw_input("Press enter to transfer files from input folder to S3...\n")

    # all files from local input folder
    path = 'input/*'
    files = glob.glob(path)

    count = 0
    for localfile in files:
        if '\\' in localfile:
            localfile = localfile.replace('\\', '/')
        k = Key(bucket)
        k.key = localfile
        print "Uploading %s to %s with key %s" % (localfile, s3_name, k.key)
        k.set_contents_from_filename(localfile)
        # print "Removing %s from local input folder" % localfile
        # remove(localfile)
        count += 1

    print "%d files uploaded to %s S3Bucket" % (count, s3_name)


# todo: ainda nao tem qualquer proteccao
# action = mapper OR reducer
def upload_map_reduce(action, s3_name, conn=S3Connection()):

    # print "\n------Connecting to S3 (Bucket: %s)...-----" % s3_name
    bucket = conn.get_bucket(s3_name)

    # all files from local folder
    path = action + '/*'
    files = glob.glob(path)

    data = {}
    op = 1
    while op not in data:
        count = 1
        for localfile in files:
            data[str(count)] = localfile
            print "\t%d.\t%s" % (count, localfile)
            count += 1

        op = raw_input("Please select a " + action + " inside " + action + " folder: ")
        # add the new file in s3 bucket
        if op in data:
            file_name = data[op].replace('\\', '/')

            k = Key(bucket)
            k.key = file_name
            k.set_contents_from_filename(file_name)
            return "s3://%s/%s" % (s3_name, file_name)
        else:
            print 'Invalid option! Please, try again...'


# todo: sem proteccoes e sem retorno sem um bucket definido atm
def select_s3_bucket(conn=S3Connection()):
    buckets = conn.get_all_buckets()

    data = {}
    op = 1
    while op not in data:
        count = 1
        print "\nAvailable buckets in your account"
        for bucket in buckets:
            data[str(count)] = bucket.name
            print "\t%d.\t%s" % (count, bucket.name)
            count += 1
        print "\t0.\tCreate new bucket (name must be unique)"

        op = raw_input("Your option: ")

        if op in data:          # return a existing bucket
            return data[op]
        elif op == str(0):
            return create_new_bucket(conn)
        else:
            print 'Invalid option! Please, try again...'


def create_new_bucket(conn=S3Connection()):
    # the new bucket will include a timestamp to try to assure a unique name to the new s3 bucket
    t = localtime(time())
    atm_date = str(t.tm_year) + str(t.tm_mon) + str(t.tm_mday) + str(t.tm_hour) + str(t.tm_min) + str(t.tm_sec)

    bucket_name = raw_input("Give a name to your bucket [it appends date of creation to assure a unique name]: ")
    conn.create_bucket(bucket_name + "-" + atm_date)

    return bucket_name + "-" + atm_date

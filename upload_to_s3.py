from boto.s3.connection import S3Connection
from boto.s3.key import Key
from time import time
from time import localtime
import glob
from os import remove


# upload every files inside local input folder. (Delete them (locally) after the upload - not at the moment)
def upload_input_data(s3_name, conn=S3Connection()):

    input_directory = select_input_directory(s3_name)

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
        bucketfile = localfile[:6] + input_directory + "/" + localfile[6:]
        if '\\' in localfile:
            localfile = localfile.replace('\\', '/')
        k = bucket.new_key(bucketfile)
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
    while op != str(0):
        count = 1
        print "\nAvailable buckets in your account"
        for bucket in buckets:
            data[str(count)] = bucket.name
            print "\t%d.\t%s" % (count, bucket.name)
            count += 1
        print "\t%d.\tCreate new bucket (name must be unique)" % count
        print "\t0.\tMain Menu"

        op = raw_input("Your option: ")
        op = op.strip()

        if op in data:          # return a existing bucket
            return data[op]
        elif op == str(count):
            return create_new_bucket(conn)
        elif op != str(0):
            print 'Invalid option! Please, try again...'


def select_s3_bucket2(conn=S3Connection()):
    buckets = conn.get_all_buckets()

    data = {}
    op = 1
    while op != str(0):
        count = 1
        print "\nAvailable buckets in your account"
        for bucket in buckets:
            data[str(count)] = bucket.name
            print "\t%d.\t%s" % (count, bucket.name)
            count += 1
        print "\t%d.\tCreate new bucket (name must be unique)" % count
        print "\t0.\tMain Menu"

        op = raw_input("Your option: ")
        op = op.strip()

        if op in data:          # return a existing bucket
            return buckets[int(op)-1]
        elif op == str(count):
            return create_new_bucket(conn)
        elif op != str(0):
            print 'Invalid option! Please, try again...'


def create_new_bucket(conn=S3Connection()):
    # the new bucket will include a timestamp to try to assure a unique name to the new s3 bucket
    t = localtime(time())
    atm_date = str(t.tm_year) + str(t.tm_mon) + str(t.tm_mday) + str(t.tm_hour) + str(t.tm_min) + str(t.tm_sec)

    bucket_name = raw_input("Give a name to your bucket [it appends date of creation to assure a unique name]: ")
    conn.create_bucket(bucket_name + "-" + atm_date)

    return bucket_name + "-" + atm_date


def create_new_directory(folders):

    op = 1
    while op == 1:
        new_folder = raw_input("Give a name to your new directory (name must be unique): ")

        if new_folder not in folders.values():
            op = 0
        else:
            print 'Invalid name! Please, try again...'

    return new_folder


def select_input_directory(bucket_name, conn=S3Connection()):

    bucket = conn.get_bucket(bucket_name)

    # just select the files inside the input folder
    folders = list(bucket.list('input/', '/'))
    # it assumes that the bucket has only one level of folders inside input folder
    folders = [f.name for f in folders if f.name.count('/') == 2]

    data = {}
    op = 1
    while op not in data:
        count = 1
        print "\nAvailable input themes in your bucket"
        for folder in folders:
            data[str(count)] = folder[6:-1]
            print "\t%d.\t%s" % (count, folder[6:-1])
            count += 1
        print "\t0.\tCreate new folder (name must be unique)"

        op = raw_input("Your option: ")

        if op in data:          # return a existing folder
            return data[op]
        elif op == str(0):
            return create_new_directory(data)
        else:
            print 'Invalid option! Please, try again...'

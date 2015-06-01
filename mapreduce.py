import time
from boto.emr.step import StreamingStep
from boto.s3.connection import S3Connection
from cluster_handler import Cluster
#from email_management import send_email
from upload_to_s3 import upload_map_reduce
from amazon_utilities import request_job_notification, connect_s3


num_of_instances = 2
instance_type = "m1.small"


# function to assist the user interface to get a answer to get a email (or not) at the end of the task
def setup_email_notification():
    op = ""
    while op != "0" and op != "1":
        print "Do yo want to receive an email with a link to download the output?"
        print"\t1.\tYes\n\t0.\tNo"
        op = raw_input("Your option: ")

    if op == '1':
        return True
    elif op == '0':
        return False
    else:
        print 'Invalid option! Please, try again...'


def list_and_select_script(bucket, action, foldername):
    files = list(bucket.list("scripts/"+foldername+"/", "/"))
    op = -1
    while op != str(0):
        count = 0
        print "\nSelect a %s script:" % action
        for file in files:
            # it only selects the scripts with the 'action' (mapper or reducer) in file name
            if action.lower() in file.name.lower():
                print "\t%d.\t%s" % (count+1, file.name[file.name.rfind('/')+1:])
                count += 1

        if count == 0:
            print "There are no %s scripts." % action
            break
        else:
            op = raw_input("Your option: ")

            try:
                int(op)
            except ValueError:
                print "Invalid operation. Try again..."
            else:
                if 0 < int(op) <= count:
                    return files[int(op)-1].name
                    break
                else:
                    print "Invalid operation. Try again..."


def select_input(bucket):

    options = list(bucket.list("input/", "/"))
    op = -1
    data = {}
    while op not in data:
        count = 1
        print "\nTheme to select a input file:"
        for key in options:
            if key.name.count('/') == 2:
                print "\t%d.\t%s" % (count, key.name[6:key.name.rfind('/')])
                data[str(count)] = key.name[6:key.name.rfind('/')]
                count += 1

        op = raw_input("Your option: ")

        try:
            int(op)
        except ValueError:
            print "Invalid operation. Try again..."
        else:
            if 0 < int(op) < count:
                files = list(bucket.list("input/"+data[op]+"/", "/"))
                while True:
                    count2 = 0
                    print "\nSelect a input file:"
                    for file in files:
                        if not(file.name.endswith("/")):
                            print "\t%d.\t%s" % (count2+1, file.name[6+len(data[op])+1:])
                            count2 += 1

                    if count2 == 0:
                        print "There are no input files."
                        op = str(-1)
                        break
                    else:
                        opt = raw_input("Your option: ")

                    try:
                        int(opt)
                    except ValueError:
                        print "Invalid operation. Try again..."
                    else:
                        if 0 < int(opt) <= count:
                            return files[int(opt)-1].name
                        else:
                            print "Invalid operation. Try again..."

            else:
                print "Invalid operation. Try again..."


def aux_to_mapreduce_to_work(bucket, action, s3_name):

    options = list(bucket.list("scripts/", "/"))
    if options and len(options) > 0:
        op = -1
        data = {}
        while op != str(0):
            count = 1
            print "\nTheme to select a %s operation:" % action
            for key in options:
                if key.name.count('/') == 2:
                    print "\t%d.\t%s" % (count, key.name[8:key.name.rfind('/')])
                    data[str(count)] = key.name[8:key.name.rfind('/')]
                    count += 1

            op = raw_input("Your option: ")

            try:
                int(op)
            except ValueError:
                print "Invalid operation. Try again..."
            else:
                if 0 < int(op) < count:
                    return "s3://%s/%s" % (s3_name, list_and_select_script(bucket, action, data[op]))
                else:
                    print "Invalid operation. Try again..."
    else:
        print 'No Themes available :( Please upload them first'


def mapreduce_to_work(cluster_handler, bucket_name, user_email, conn_s3=connect_s3()):

    # list all mapreduce options available (based on mapper folder)
    bucket = conn_s3.get_bucket(bucket_name)

    input_dir = 's3://%s/%s' % (bucket_name, select_input(bucket))
    print "input: %s" % input_dir

    s3_mapper_dir = aux_to_mapreduce_to_work(bucket, 'MAPPER', bucket_name)
    print "mapper: %s" % s3_mapper_dir
    s3_reducer_dir = aux_to_mapreduce_to_work(bucket, 'REDUCER', bucket_name)
    print "reducer: %s" % s3_reducer_dir

    # input_dir = 's3://%s/input/' % bucket_name
    output_dir = 's3://%s/output' % bucket_name

    execute_step(cluster_handler, s3_mapper_dir, input_dir, s3_reducer_dir, user_email, output_dir)


def execute_step(cluster_handler, s3_mapper_dir, input_dir, s3_reducer_dir, user_email, output_dir, s3_combiner_dir=None):

    # we will only support 1 step per cluster at time
    step, downloadlink = create_step(s3_mapper_dir, s3_reducer_dir, input_dir, output_dir, s3_combiner_dir)

    # get a cluster (a existing cluster and active (True) or creating a new one)
    id_cluster = cluster_handler.input_select_create_cluster()
    # associate the created step to the chosen cluster. It starts with add_jobflow_steps
    step_object = cluster_handler.conn_emr.add_jobflow_steps(id_cluster, [step])
    # get our step id from. It is always the 1st (position 0), because we'll only support one step per task
    id_step = step_object.stepids[0].value
    print "\nStep %s is running in %s cluster....\n" % (step.name, id_cluster)

    mail_desired = setup_email_notification()
    if mail_desired:
        request_job_notification({'link': downloadlink,
                                  'step_id': id_step,
                                  'cluster_id': id_cluster,
                                  'recipient': user_email})

        print 'You will be notified when the step is completed!'
        time.sleep(4)

        # ok = describe_step(clustes_id, step_id) ok.status.state "COMPLETED"


# The input and output locations are default
# It includes the directories of mapper and reducer, selected by user
def create_step(s3_mapper_dir, s3_reducer_dir, input_dir, output_dir, s3_combiner_dir):

    step_name = raw_input("\nGive a name to your step: ")

    t = time.localtime(time.time())
    datetime = str(t.tm_year) + str(t.tm_mon) + str(t.tm_mday) + str(t.tm_hour) + str(t.tm_min) + str(t.tm_sec)
    outputlocation = '%s/%s_%s' % (output_dir, step_name, datetime)

    print "Creating %s step..." % step_name

    if s3_combiner_dir:
        combiner = s3_combiner_dir

    else:
        combiner = s3_reducer_dir

    step = StreamingStep(name=step_name,
                         mapper=s3_mapper_dir,
                         reducer=s3_reducer_dir,
                         combiner=combiner,
                         input=input_dir,
                         output=outputlocation,
                         action_on_failure='CONTINUE',
                         step_args=['-numReduceTasks', '1'])

    print "Step created: " + step_name

    return step, outputlocation + "/part-00000"

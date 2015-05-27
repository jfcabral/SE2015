import time
from boto.emr.step import StreamingStep
from boto.s3.connection import S3Connection
from cluster_handler import Cluster
#from email_management import send_email
from upload_to_s3 import upload_map_reduce
from amazon_utilities import request_job_notification, set_folder_public_simple, connect_s3


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


def aux_to_mapreduce_to_work(bucket, action, s3_name):
    op = "B"
    while op != "" and op.upper() != "A":
        i = 0
        print "\nSelect a %s operation:" % action
        options = bucket.list(prefix=action)
        data = {}
        for key in options:
            data[str(i)] = key.name
            # todo: melhorar esta parte; apresentar so o que esta dentro da pasta (tirar o prefixo)
            print "\t%d.\t%s" % (i, key.name.encode('utf-8'))
            i += 1

        print "\tA.\tUpload a new one"
        print "Enter.\tTo go back to the previous menu\n"

        op = raw_input("Your option: ")

        if op in data:
            return "s3://%s/%s" % (s3_name, data[op])
        elif op.upper() == "A":
            print "Upload a new %s operation" % action
            return upload_map_reduce(action, s3_name)
        else:
            print 'Invalid option! Please, try again...'


def mapreduce_to_work(cluster_handler, bucket_name, user_email, conn_s3=connect_s3()):
    # list all mapreduce options available (based on mapper folder)
    bucket = conn_s3.get_bucket(bucket_name)

    s3_mapper_dir = aux_to_mapreduce_to_work(bucket, 'mapper/', bucket_name)
    print "mapper: %s" % s3_mapper_dir
    s3_reducer_dir = aux_to_mapreduce_to_work(bucket, 'reducer/', bucket_name)
    print "reducer: %s" % s3_reducer_dir

    input_dir = 's3://%s/input/' % bucket_name
    output_dir = 's3://%s/output/' % bucket_name

    execute_step(cluster_handler, s3_mapper_dir, input_dir, s3_reducer_dir, user_email, output_dir, conn_s3)


# TODO selecionar input folder antes?
def execute_step(cluster_handler, s3_mapper_dir, input_dir, s3_reducer_dir, user_email, output_dir, conn_s3):
    # we will only support 1 step per cluster at time
    step, downloadlink = create_step(s3_mapper_dir, s3_reducer_dir, input_dir, output_dir, conn_s3)

    if user_email:
        # Make sure the link is accessible
        set_folder_public_simple(downloadlink, conn_s3)

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

        """
        print("\nPlease wait. You'll receive feedback (every 20 seconds) until the end...")
        while True:
            # get step.status.state, given a specific cluster and step ids
            step_status = emr_conn.describe_step(id_cluster, id_step)
            step_state = step_status.status.state
            time.sleep(30)
            if step_state == 'COMPLETED':
                send_email(user_email, downloadlink, mail_conn)
                # tornar o ficheiro acessivel ao publico
                set_file_public(s3_name, downloadlink, s3_conn)
                print("\nMail has been sent to %s!" % user_email)
                break
            else:
                print step_state
        """

        # ok = describe_step(clustes_id, step_id) ok.status.state "COMPLETED"


# The input and output locations are default
# It includes the directories of mapper and reducer, selected by user
def create_step(s3_mapper_dir, s3_reducer_dir, input_dir, output_dir, conn_s3):

    step_name = raw_input("\nGive a name to your step: ")

    t = time.localtime(time.time())
    datetime = str(t.tm_year) + str(t.tm_mon) + str(t.tm_mday) + str(t.tm_hour) + str(t.tm_min) + str(t.tm_sec)
    outputlocation = '%s/%s_%s' % (output_dir, step_name, datetime)

    print "Creating %s step..." % step_name

    step = StreamingStep(name=step_name,
                         mapper=s3_mapper_dir,
                         reducer=s3_reducer_dir,
                         combiner=s3_reducer_dir,
                         input=input_dir,
                         output=outputlocation,
                         step_args=['-numReduceTasks', '1'])

    print "Step created: " + step_name

    return step, outputlocation + "/part-00000"

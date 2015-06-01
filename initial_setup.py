from os import listdir
from time import sleep
from os.path import isdir


from upload_to_s3 import select_s3_bucket2
from amazon_utilities import connect_s3, connect_beanstalk
from dynamo_handler import DynamoHandler


BEANSTALK_APP_NAME = 'email-dispatcher-app'
BEANSTALK_TEMPLATE = 'email_dispatcher'


MAP_REDUCE_DATA = './data/'
SLEEP_TIME_BEFORE_POPULATING_PROFILES = 5


def setup_beanstalk():
    print 'Beanstalk:'
    conn_beanstalk = connect_beanstalk()
    ret = conn_beanstalk.describe_applications(application_names=[BEANSTALK_APP_NAME])
    apps = ret['DescribeApplicationsResponse']['DescribeApplicationsResult']['Applications']

    if len(apps) == 1:
        print '\tEmail Dispatcher application already exists!'

        if BEANSTALK_TEMPLATE in apps[0]['ConfigurationTemplates']:
            print '\tTemplate exists'

        environments = conn_beanstalk.describe_environments(BEANSTALK_APP_NAME)
        if environments and len(environments['DescribeEnvironmentsResponse']['DescribeEnvironmentsResult']['Environments']) == 0:
            print '\tDon\'t forget to setup an environment with the Dockerfile ' \
                  'in https://console.aws.amazon.com/elasticbeanstalk/home?region=us-east-1'

    else:
        conn_beanstalk.create_application(BEANSTALK_APP_NAME)
        print '\tEmail dispatcher app was created, please setup an environment with the Dockerfile ' \
              'in https://console.aws.amazon.com/elasticbeanstalk/home?region=us-east-1'


def setup_dynamo_db():
    print 'Dynamo'
    handler = DynamoHandler()
    handler.check_create_table()
    print
    return handler


def upload_folder_rec(bucket, folder):
    contents = listdir(folder)
    for elem in contents:
        path = '%s/%s' % (folder, elem)

        if isdir(path):
            upload_folder_rec(bucket, path)
        else:
            k = bucket.new_key('%s/%s' %(folder[2:], elem))
            k.set_contents_from_filename(path)


def upload_dynamo_profiles(handler):
    sleep(SLEEP_TIME_BEFORE_POPULATING_PROFILES)  # sleep some time to ensure the table was created

    conn_s3 = connect_s3()

    # select a bucket
    bucket = select_s3_bucket2(conn_s3)

    print 'Uploading data, please wait...'

    # Upload Map Reduce data (inputs / scripts
    upload_folder_rec(bucket, MAP_REDUCE_DATA)

    # check profile existence
    profile_name = 'Word Count'
    if not handler.check_if_profile_exists(profile_name):
        print '\nDynamo Profiles\n\tPopulating with Word Count'
        base_path = 's3://%s/%s/%s/' % (bucket.name, MAP_REDUCE_DATA[2:], profile_name.replace(' ', '_'))

        # Word Count profile
        handler.create_profile(profile_name,
                               base_path + 'input/',
                               base_path + 'scripts/WordCountMapper.py',
                               base_path + 'scripts/WordCountReducer.py')

    """if not handler.check_if_profile_exists('IN'):
        print '\nDynamo Profiles\n\tPopulating with IN Drug category forecast preparation'
        base_path = 's3://%s/' % bucket.name

        # Word Count profile
        handler.create_profile('IN',
                               base_path + 'input/',
                               base_path + 'scripts/IN_Mapper.py',
                               base_path + 'scripts/IN_Reducer.py',
                               base_path + 'scripts/IN_Combiner.py')"""

if __name__ == '__main__':
    dynamo_handler = setup_dynamo_db()
    setup_beanstalk()
    upload_dynamo_profiles(dynamo_handler)

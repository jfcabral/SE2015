# coding=utf-8
from ctypes.macholib.dyld import dyld_default_search
import sys

import boto.ses
from boto.s3.connection import S3Connection

from cluster_handler import Cluster
from dynamo_handler import DynamoHandler
from email_management import verify_email
from upload_to_s3 import select_s3_bucket
from amazon_utilities import connect_emr, connect_s3, set_folder_public

from mapreduce import mapreduce_to_work, execute_step
from upload_to_s3 import upload_input_data
from email_management import send_email


class Menu:
    REGION_DEFAULT = 'us-east-1'

    def __init__(self):
        self.conn_emr = connect_emr()
        self.conn_mail = boto.ses.connect_to_region(Menu.REGION_DEFAULT)
        self.conn_s3 = connect_s3()

        self.cluster_handler = Cluster(self.conn_emr)
        self.dynamo_handler = DynamoHandler()

        self.user_email = None

    def main_menu(self):
        op = -1
        while op != '0':
            print "\nWelcome to Services Engineering Project\n\nMain Menu"
            print "1.\tClusters\n2.\tRun task\n0.\tExit"

            ops = {'1': 'self.cluster_handler.menu()',
                   '2': 'self.run_simple_flow()'}

            op = raw_input('Your option: ')

            if op in ops:
                eval(ops[op])
            elif op != '0':
                print 'Invalid option! Please, try again...'

        print '\nThank you!'

    def run_simple_flow(self):

        print "\n--------- Run simple task ---------"

        # todo: remove/adjust this code to the 2nd milestone
        # this piece of code belongs to our SES idea. It will be here to this milestone
        # our architecture contemplates another flow
        self.user_email = raw_input("Please insert your email: ")

        mail_response = verify_email(self.user_email, self.conn_mail)
        if mail_response:
            print "Your email already was kept here! :) Thank you!"
        else:
            print "AWS sent to you a confirmation email. Please confirm in your email account! ASAP! Thank you!\n"

        print "\nFirst you have to configure an s3 bucket for output"
        bucket_name = select_s3_bucket(self.conn_s3)
        print "\nBucket chosen: %s\n" % bucket_name

        op = -1

        # TODO maybe improve
        while op not in ['0', '1', '2', '3']:
            print "This flow includes:"
            print "\t1.\tUpload files to S3 Bucket\n" \
                  "\t2.\tSelect or upload a MapReduce operation\n" \
                  "\t3.\tSelect an existing Map-Reduce profile\n" \
                  "\t0.\tMain Menu"

            op = raw_input('Your option: ')

            ops = {'1': 'upload_input_data(bucket_name)',
                   '2': 'mapreduce_to_work(self.cluster_handler, bucket_name, self.user_email, self.conn_s3)',
                   '3': 'self.handle_profiles(self.dynamo_handler, bucket_name)'}

            if op in ops:
                eval(ops[op])
            else:
                print 'Invalid option! Please, try again...'

    def handle_profiles(self, dynamo_handler, bucket_name):
        profile_data = dynamo_handler.list_input_profile()
        #print profile_data

        if profile_data:
            output_dir = 's3://%s/output/' % bucket_name

            # TODO resolver questão da selecão input folder! permitir escolher ou usar default (como esta)
            # TODO temporary hack until ^ is solved
            execute_step(self.cluster_handler,
                         profile_data['mapper_file_path'],
                         profile_data['sample_path'],
                         profile_data['reducer_file_path'],
                         self.user_email,
                         output_dir)


if __name__ == '__main__':
    Menu().main_menu()

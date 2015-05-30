# coding=utf-8

import sys

import boto.ses
import re
from cluster_handler import Cluster
from dynamo_handler import DynamoHandler
from email_management import verify_email
from upload_to_s3 import select_s3_bucket
from amazon_utilities import connect_emr, connect_s3
from mapreduce import mapreduce_to_work, execute_step

from upload_to_s3 import upload_input_data  # it is used


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

        if self.user_email is None:

            is_valid = None
            while is_valid is None:
                self.user_email = raw_input("Please insert your email: ")
                is_valid = re.match('\w+[.|\w]\w+@\w+[.]\w+[.|\w+]\w+', self.user_email)

                # is_valid = validate_email(self.user_email)
                if is_valid is None:
                    print ("Invalid email! Please try again\n")

            mail_response = verify_email(self.user_email, self.conn_mail)
            if mail_response:
                print "Your email already was kept here! :) Thank you!"
            else:
                print "AWS sent to you a confirmation email. Please confirm in your email account! ASAP! Thank you!\n"

        print "\nFirst you have to configure an s3 bucket for output"
        bucket_name = select_s3_bucket(self.conn_s3)
        print "\nBucket chosen: %s\n" % bucket_name

        if bucket_name is not None:

            op = -1

            # TODO maybe improve
            while op != str(0):
                print "This flow includes:"
                print "\t1.\tUpload files to S3 Bucket\n" \
                      "\t2.\tSelect or upload a MapReduce operation\n" \
                      "\t3.\tSelect an existing Map-Reduce profile\n" \
                      "\t0.\tMain Menu"

                op = raw_input('Your option: ')

                ops = {'1': 'upload_input_data(bucket_name)',
                       '2': 'mapreduce_to_work(self.cluster_handler, bucket_name, self.user_email, self.conn_s3)',
                       '3': 'self.handle_profiles(bucket_name)'}

                if op in ops:
                    eval(ops[op])
                elif op != str(0):
                    print 'Invalid option! Please, try again...'

    def handle_profiles(self, bucket_name):
        profile_data = self.dynamo_handler.list_input_profile()
        #print profile_data

        if profile_data:
            output_dir = 's3://%s/output' % bucket_name

            #input_dir = self.choose_input_dir()

            if 'combiner_file_path' in profile_data:

                # TODO resolver questão da selecão input folder! permitir escolher ou usar default (como esta)
                # TODO temporary hack until ^ is solved
                execute_step(self.cluster_handler,
                             profile_data['mapper_file_path'],
                             profile_data['sample_path'],
                             profile_data['reducer_file_path'],
                             self.user_email,
                             output_dir,
                             profile_data['combiner_file_path'])
            else:
                execute_step(self.cluster_handler,
                             profile_data['mapper_file_path'],
                             profile_data['sample_path'],
                             profile_data['reducer_file_path'],
                             self.user_email,
                             output_dir)

    def choose_input_dir(self, sample_dir):
        print 'Choose the input folder:\n1 - Use profile sample\n2 - '


if __name__ == '__main__':
    Menu().main_menu()

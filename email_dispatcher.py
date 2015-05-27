import json
from time import sleep

from email_management import send_email
from amazon_utilities import connect_emr, connect_ses, connect_s3, connect_sqs, create_get_queue, set_folder_public_simple, QUEUE_EMAIL_DISPATCH


TIME_WAIT_FOR_MESSAGES = 20  # 20s is the maximum allowed by amazon
SLEEP_TIME_AFTER_EMAIL = 15  # in seconds


# Allows to send email notifications in regard to a step
# Receives email notification requests from a Queue
class EmailDispatcher:

    def __init__(self):
        self.jobs = []
        self.conn_emr = connect_emr()
        self.conn_ses = connect_ses()
        self.conn_s3 = connect_s3()

    def init_mail_dispatch(self, queue):

        while True:
            # read message from queue
            print 'waiting for messages'
            message = queue.read(wait_time_seconds=TIME_WAIT_FOR_MESSAGES)

            if message:
                print 'New Message!'

                # get message content
                body = message.get_body()
                #print body

                # parse message {'recipient', 'link', 'step_id', 'job_id'}
                self.jobs.append(json.loads(body))

                # delete message
                queue.delete_message(message)

                sleep(SLEEP_TIME_AFTER_EMAIL)

            # check if a job has finished
            self.check_jobs()

    def check_jobs(self):

        for elem in self.jobs:

            if 'link' in elem and 'recipient' in elem and 'step_id' in elem and 'cluster_id' in elem:
                step_info = self.conn_emr.describe_step(elem['cluster_id'], elem['step_id'])

                step_data = self.parse_step_info(elem['link'], step_info)

                if step_data:
                    if step_info.status.state == 'COMPLETED':
                        # Make sure the link is accessible
                        set_folder_public_simple(elem['link'], self.conn_s3)

                    send_email(elem['recipient'], step_data, self.conn_ses)
                    self.jobs.remove(elem)

    # returns a dictionary containing all data relevant to the step
    def parse_step_info(self, url, step_info):
        ret = None

        if step_info.status.state in ['COMPLETED', 'TERMINATING', 'TERMINATED']:
            ret = {'step_id': step_info.id,
                   'step_name': step_info.name,
                   'status': step_info.status.state,
                   'creation_date': step_info.status.timeline.creationdatetime,
                   'end_date': step_info.status.timeline.enddatetime,
                   'link': url}

        elif step_info.status.state in ['TERMINATED_WITH_ERRORS', 'FAILED', 'CANCELLED', 'SHUTTING_DOWN']:
            ret = {'step_id': step_info.id,
                   'step_name': step_info.name,
                   'status': step_info.status.state,
                   'creation_date': step_info.status.timeline.creationdatetime}

        return ret


if __name__ == '__main__':
    # connect SQS
    conn_sqs = connect_sqs()
    # setup queue (create if it doesn't exit)
    queue = create_get_queue(QUEUE_EMAIL_DISPATCH, conn_sqs)
    # start listening from the queue and sending emails
    EmailDispatcher().init_mail_dispatch(queue)
    """conn_emr = connect_emr()
    step_info = conn_emr.describe_step('j-3O983JX5SNZBI', 's-9V22DHFEBQB4')
    EmailDispatcher().parse_step_info('fjdsfjhsdjfhsljflskxhgds', step_info)
    print step_info"""

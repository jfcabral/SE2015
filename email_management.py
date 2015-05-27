import time

from amazon_utilities import connect_ses

EMAIL_SENDER = 'is.a2.2014.jm@gmail.com'


def verify_email(email, mail_conn=connect_ses()):
    verified_list = mail_conn.list_verified_email_addresses()

    # get the list of verified emails
    verified_emails = verified_list['ListVerifiedEmailAddressesResponse']['ListVerifiedEmailAddressesResult'][
        'VerifiedEmailAddresses']

    if email in verified_emails:
        return True
    else:
        mail_conn.verify_email_address(email)
        return False


def send_email(email, step_data, conn_ses=connect_ses()):
    if verify_email(email, conn_ses):

        step_data['creation_date'] = step_data['creation_date'][:-8].replace("-", "/").replace("T", " ")

        # success case (has link)
        if 'link' in step_data:

            step_data['end_date'] = step_data['end_date'][:-8].replace("-", "/").replace("T", " ")

            t = time.localtime(time.time())
            datetime = "%s/%s/%s %s:%s:%s" % (str(t.tm_mday), str(t.tm_mon), str(t.tm_year), str(t.tm_hour), str(t.tm_min), str(t.tm_sec))

            subject = "[ES - MapReduce] - Download link - %s" % datetime

            # use the valid url to access the file in the specific bucket
            link = "http://s3.amazonaws.com/" + step_data['link'][5:]

            html = """\
            <html>
                <head></head>
                <style>
                    body {padding:10%; background-color:#2c3e50; color:#ededed; text-align: justify; text-justify: inter-word;}
                </style>
                <body style="padding:10%; background-color:#2c3e50; color:#ededed; text-align: justify; text-justify: inter-word;">
                    <h1 style="color:#ffffff;">:)</h1>
                    <br>
                    <h1 style="color:#ffffff;">Welcome to our MapReduce solution</h1>
                    <p>Your MapReduce work is completed! Please download the output</p>
                    <hr style="width:100%; border:2px solid #ffffff; text-align:center;">
                    <h1>""" + link + """</h1>
                    <hr style="width:100%; border:2px solid #ffffff; text-align:center;">
                    <h3 style="color:#ffffff;">Report:</h3>
                    <p><b>Step ID:</b> """ + step_data['step_id'] + "</p>\
                    <p><b>Step name:</b> """ + step_data['step_name'] + "</p>\
                    <p><b>Status:</b> """ + step_data['status'] + "</p>\
                    <p><b>Started:</b> """ + step_data['creation_date'] + "</p>\
                    <p><b>Finished:</b> """ + step_data['end_date'] + "</p>\
                </body>\
            </html>\
            """

            # use the valid url to access the file in the specific bucket
            body = "http://s3.amazonaws.com/" + step_data['link'][5:] + "\nStep ID: " + step_data[
                'step_id'] + "\nStep name: " + step_data['step_name'] + "\nStatus: " + step_data[
                'status'] + "\nStarted: " + step_data['creation_date'] + "\nFinished: " + step_data['end_date']

            conn_ses.send_email(EMAIL_SENDER, subject, body, email, html_body=html)

            print "[Completed] Mail has been successfully sent to " + email

        # error case -> no link!
        else:

            subject = "[ES - MapReduce] - Your work has failed"

            html = """\
            <html>
                <head></head>
                <style>
                    body {padding:10%; background-color:#e74c3c; color:#ededed; text-align: justify; text-justify: inter-word;}
                </style>
                <body style="padding:10%; background-color:#e74c3c; color:#ededed; text-align: justify; text-justify: inter-word;">
                    <h1 style="color:#ffffff;">:(</h1>
                    <br>
                    <h1 style="color:#ffffff;">Welcome to our MapReduce solution</h1>
                    <p>Your MapReduce work has failed! Please try again...</p>
                    <hr style="width:100%; color=#ffffff; border:2px solid #ffffff; text-align:center;">
                    <h3 style="color:#ffffff;">Report:</h3>
                    <p><b>Step ID:</b> """ + step_data['step_id'] + "</p>\
                    <p><b>Step name:</b> """ + step_data['step_name'] + "</p>\
                    <p><b>Status:</b> """ + step_data['status'] + "</p>\
                    <p><b>Started:</b> """ + step_data['creation_date'] + "</p>\
                </body>\
            </html>\
            """

            # use the valid url to access the file in the specific bucket
            body = "Your MapReduce has failed! Please try again...\nStep ID: " + step_data[
                'step_id'] + "\nStep name: " + step_data['step_name'] + "\nStatus: " + step_data[
                'status'] + "\nStarted: " + step_data['creation_date'] + "\nFinished: " + step_data['end_date']

            conn_ses.send_email(EMAIL_SENDER, subject, body, email, html_body=html)

            print "[Error] Mail has been successfully sent to " + email

    else:
        print "You didn't verified your email...so we will not send any email with a link to download the output!"


if __name__ == '__main__':
    # AWS SES demands you to verify all emails before sending / receiving any
    # verify_email(EMAIL_SENDER)

    ret = {'step_id': 'slb1904',
           'step_name': 'Mantorras',
           'status': 'Working',
           'creation_date': '2015-05-26T22:09:00.213Z',
           'end_date': '2015-05-26T22:17:41.642Z',
           'link': 'www.slbenfica.pt'}
    send_email("marcocsp@hotmail.com", ret)

    ret2 = {'step_id': 'slb1904',
            'step_name': 'Mantorras',
            'status': 'Working',
            'creation_date': '2015-05-26T22:09:00.213Z',
            'end_date': '2015-05-26T22:17:41.642Z'}
    send_email("marcocsp@hotmail.com", ret2)

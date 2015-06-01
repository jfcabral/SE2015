Service Engineering Project

Map-Reduce App

2015

## 1. Requirements

* Python 2.7.x
* Boto 2.38 (latest available at the time of delivery)

Note: Assuming that boto.cfg is properly configured with the user AWS access keys

## 2. Setup

### 2.1. Setup script

```sh
    python initial_setup.py
```

This script will handle the basic configuration required to exemplify some of the project functionalities. It is responsible for:

* Creating the Map-Reduce Profiles in Dynamo DB
* Upload examples of Map-Reduce scripts and inputs to S3
* Create an Elastic Beanstalk Application to host the Email Dispatcher module
* Populate the Map-Reduce DB with 1 default profile (Word Count)

### 2.2. Manually setup of the Elastic Beanstalk Environment

Although the previous step creates an application in the Elastic Beanstalk Amazon service, it is manually required to create an environment to deploy the application. Efforts were made to implement this in a fully programmatic way, yet the lack of examples and the complexity of the configurations involved, prevented us from achieving it. An attempt was made to export (using boto) a previously created environment from the Web interface, but unfortunately, the configurations obtained by boto were incomplete and could not be used to recreate the environment programmatically.
Nevertheless, the following steps are required to deploy the Docker container:

* Open the file placed in: '/beanstalk/Dockerfile_sample'
* Edit it, specifying the user AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
* Navigate to https://console.aws.amazon.com/elasticbeanstalk/home?region=us-east-1
* Select the email-dispatcher-app created by the setup script 
* Select Create New Environment in the upper right corner
* Select Create web server
* Use default permission (next)
* Select 'Docker' as Predefined configuration and 'Single instance' in the Environment type
* For the Application Version choose Upload your own and upload the Dockerfile edited previously
* Click next 2 times
* Select t1.micro or t2.micro as the Instance type and press next 2 times
* Click Launch

## 3. First run

```sh
    python main_menu.py
```

This will take you to the main menu, allowing you to Manage Clusters using option 1, and run Map-Reduce talks using option 2. 

Note: By default, in order to send emails using Amazon Simple Email Service, it is required for the recipient to be previously verified. Therefore when the user selects the 'run task' option, it is prompted to introduce his email, so it can be verified in an early stage, allowing a new user to receive a notification when his step finishes or aborts.


__author__ = 'Joni'

from time import sleep
from datetime import datetime

from boto.emr.step import StreamingStep
from boto.emr.connection import EmrConnection
from boto.emr.instance_group import InstanceGroup
from boto.emr.bootstrap_action import BootstrapAction


#S3_BUCKET = 'eng-serv-2015'
bucket_name = 'eng-serv-teste3'


NUM_INSTANCES = 1

job_ts = datetime.now().strftime("%Y%m%d%H%M%S")
emr = EmrConnection()
wc_step = StreamingStep(name = 'wc text',
                        mapper =('s3://%s/mapper.py' % bucket_name),
                        combiner =('s3://%s/reducer.py' % bucket_name),
                        reducer =('s3://%s/reducer.py' % bucket_name),
                        input=('s3://%s/input' % bucket_name),
                        step_args=['-numReduceTasks', '1'],
                        output='s3://%s/output-%s' %(bucket_name, str(datetime.now())))

#print wc_step

#bootstrap_step = BootstrapAction("download.tst", "s3://elasticmapreduce/bootstrap-actions/download.sh",None)

"""instance_groups = [
    InstanceGroup(1, 'MASTER', 'm1.small', 'SPOT', 'master-spot@0.20', '0.01'),
    InstanceGroup(1, 'CORE', 'm1.small', 'SPOT', 'core-spot@0.20', '0.01')
    ]"""
#num_instances, role, type, market, name,
instance_groups = [
    InstanceGroup(1, 'MASTER', 'm1.small', 'ON_DEMAND', 'Master'),
    #InstanceGroup(1, 'TASK', 'm1.small', 'ON_DEMAND', 'Task'),
    InstanceGroup(1, 'CORE', 'm1.small', 'ON_DEMAND', 'Core')
]

jf_id = emr.run_jobflow(log_uri='s3://%s/logs' %(bucket_name),
                        name='wc jobflow',
                        steps=[wc_step],
                        #num_instances=NUM_INSTANCES,
                        #master_instance_type='m1.small',
                        #slave_instance_type='m1.small',
                        instance_groups=instance_groups,
                        job_flow_role = 'EMR_EC2_DefaultRole',
                        #bootstrap_actions=[bootstrap_step],
                        service_role = 'EMR_DefaultRole',
                        action_on_failure='CONTINUE',
                        visible_to_all_users="True",
                        ami_version = '2.4',
                        hadoop_version='1.0.3',
                        keep_alive=True)

emr.set_termination_protection(jf_id, True)

print jf_id

while True:
    jf = emr.describe_jobflow(jf_id)
    #print "[%s] %s" % (datetime.now().strftime("%Y-%m-%d %T"), jf.state)
    print datetime.now(), jf.state
    if jf.state == 'COMPLETED':
        break
    sleep(30)
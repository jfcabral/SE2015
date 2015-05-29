from time import sleep
from sys import stderr, exc_info
from upload_to_s3 import select_s3_bucket
from boto.emr.connection import EmrResponseError
from boto.emr.instance_group import InstanceGroup
from amazon_utilities import ACTIVE_CLUSTER_STATES, ALL_CLUSTER_STATES, RESIZABLE_CLUSTER_STATES, connect_emr


class Cluster:
    # Maximum allowed Task instances for a Cluster
    MAX_TASK_INSTANCES = 5
    # Maximum allowed Core instances for a Cluster
    MAX_CORE_INSTANCES = 3
    # Hadoop version 1.x (required to allow the use of m1.small instances)
    EMR_HADOOP_VERSION = '1.0.3'
    # Amazon AMI last version to support Hadoop 1
    EMR_AMI_VERSION = '2.4'

    DEFAULT_INSTANCE_GROUPS = [
        InstanceGroup(1, 'MASTER', 'm1.small', 'ON_DEMAND', 'Master'),
        InstanceGroup(1, 'CORE', 'm1.small', 'ON_DEMAND', 'Core'),
        InstanceGroup(1, 'TASK', 'm1.small', 'ON_DEMAND', 'Task')
    ]

    def __init__(self, conn=connect_emr()):
        self.conn_emr = conn

    def menu(self):
        op = -1
        while op != 0:
            print "\n---------- Clusters ----------"
            print "1.\tList Active Clusters\n" \
                  "2.\tList All Clusters\n" \
                  "3.\tCreate Cluster\n" \
                  "4.\tResize Cluster\n" \
                  "5.\tTerminate Cluster\n\n" \
                  "0.\tMain Menu\n"

            ops = {1: 'self.list_clusters()',
                   2: 'self.list_clusters(True)',
                   3: 'self.create_cluster()',
                   4: 'self.handle_cluster_resize()',
                   5: 'self.terminate_cluster()'}

            op = input('Your option: ')

            if op in ops:
                print
                eval(ops[op])
                sleep(2)
            elif op != 0:
                print 'Invalid option! Please, try again...'

    def create_cluster(self):
        print 'Please select a bucket to save the logs '
        s3_bucket_log = select_s3_bucket()

        _name = raw_input('\tCluster Name: ')

        cluster_id = self.conn_emr.run_jobflow(log_uri='s3://%s/logs' % s3_bucket_log,
                                               name=_name,
                                               steps=[],
                                               # num_instances=1,
                                               instance_groups=Cluster.DEFAULT_INSTANCE_GROUPS,
                                               job_flow_role='EMR_EC2_DefaultRole',
                                               service_role='EMR_DefaultRole',
                                               ami_version=Cluster.EMR_AMI_VERSION,
                                               hadoop_version=Cluster.EMR_HADOOP_VERSION,
                                               action_on_failure='CONTINUE',  # TODO careful!
                                               keep_alive=True)

        if cluster_id:
            print 'Created cluster: ', cluster_id

        return cluster_id

    def terminate_cluster(self):
        cluster = self.input_select_cluster()

        if cluster:
            try:
                self.conn_emr.terminate_jobflow(cluster.id)
                print 'Cluster %s was successfully terminated after %s normalized hours.' \
                      % (cluster.id, cluster.normalizedinstancehours)
            except EmrResponseError:
                print >> stderr, 'Error terminating Cluster', exc_info()[1].message

    def input_select_cluster(self):
        ret = None
        cluster_list = self.list_clusters()

        if cluster_list and len(cluster_list) > 0:
            while True:
                cluster_op = input('Please select an option (-1 to go back): ')

                if cluster_op < -1 or cluster_op > len(cluster_list):
                    print >> stderr, 'Invalid option'
                else:
                    break

            if cluster_op != -1:
                ret = cluster_list[cluster_op]

        return ret

    def input_select_create_cluster(self):
        cluster_id = None
        cluster_list = self.list_clusters(False, True)
        index_create_new_cluster = len(cluster_list)

        print '--------------------------------\n\t%d.\tCreate new Cluster' % index_create_new_cluster

        while True:
            try:
                cluster_op = int(raw_input('Please select an option (-1 to go back): '))

                if cluster_op == index_create_new_cluster:
                    cluster_id = self.create_cluster()
                    break

                elif cluster_op == -1:
                    cluster_id = -1
                    break

                elif cluster_list and 0 <= cluster_op < len(cluster_list):
                    cluster_id = cluster_list[cluster_op].id
                    break

                else:
                    print >> stderr, 'Invalid option'

            except ValueError:
                print >> stderr, 'Invalid option'

        return cluster_id

    def handle_cluster_resize(self):
        cluster = self.input_select_cluster()

        if cluster:
            instances = self.conn_emr.list_instance_groups(cluster.id)

            if not instances or len(instances.instancegroups) == 0:
                print >> stderr, 'No instances were found.'

            else:
                instance = self.print_input_instance_group(instances)

                if instance:
                    if instance.instancegrouptype == 'MASTER':
                        print >> stderr, 'Master instances can not be resided!'

                    elif instance.instancegrouptype in 'CORE':
                        self.__handle_core_increase(instance)

                    elif instance.instancegrouptype == 'TASK':
                        self.__handle_task_resize(instance)

                    else:
                        print >> stderr, 'Unknown Instance Group Type ', instance.instancegrouptype

    def __handle_task_resize(self, instance):
        print '\tCurrent instances: ', instance.runninginstancecount
        number_instances = input('\tNew number of instances: ')

        if 0 <= number_instances <= Cluster.MAX_TASK_INSTANCES:
            try:
                self.conn_emr.modify_instance_groups(instance.id, number_instances)

                print 'Task instance number was successfully changed from %s to %d!' \
                      % (instance.runninginstancecount, number_instances)
                sleep(1)

            except EmrResponseError:
                print >> stderr, 'Error occurred when increasing core size ', exc_info()[1].message
        else:
            print >> stderr, 'Invalid option'

    def __handle_core_increase(self, instance):
        print 'Attention: Core instances can only be increased!\n\tCurrent instances: ', instance.runninginstancecount
        number_instances = input('\tNew number of instances: ')

        if int(instance.runninginstancecount) < number_instances <= Cluster.MAX_CORE_INSTANCES:
            try:
                self.conn_emr.modify_instance_groups(instance.id, number_instances)

                print 'Core instance number was successfully increased from %s to %d!' \
                      % (instance.runninginstancecount, number_instances)
                sleep(2)

            except EmrResponseError:
                print >> stderr, 'Error occurred when increasing core size', exc_info()[1].message
        else:
            print >> stderr, 'Invalid option'

    def print_input_instance_group(self, instances):
        i = 0
        resizable_instance = False

        print "\n\n\t---------- Groups ----------"

        for instance in instances.instancegroups:
            if instance.status.state in RESIZABLE_CLUSTER_STATES:
                resizable_instance = True

            print '\t%d)\n\t\tType:\t%s\n\t\tID:\t%s\n\t\tType:\t%s - %s\n\t\tRequested / Running Count:\t%s/%s\n' \
                  '\t\tStatus:\t%s\n\t------------------------------------' \
                  % (i, instance.instancegrouptype, instance.id, instance.instancetype, instance.market,
                     instance.requestedinstancecount,
                     instance.runninginstancecount, instance.status.state)

            i += 1

        op = -1

        if resizable_instance:
            while True:
                try:
                    op = int(raw_input('Select a Instance Group (-1 to go back): '))

                    if op < -1 or op >= len(instances.instancegroups) \
                            or instances.instancegroups[op].status.state not in RESIZABLE_CLUSTER_STATES:
                        raise ValueError()
                    else:
                        break
                except ValueError:
                    print >> stderr, 'Invalid option'
        else:
            print 'No Running or Waiting (resizable) instances were found at the moment. Try a bit later.'

        if op != -1:
            ret = instances.instancegroups[op]
        else:
            ret = None

        return ret

    def list_instances(self, cluster):
        temp = self.conn_emr.list_instances(cluster.id)
        print temp

    def list_clusters(self, all_states=False, silent=False):
        if all_states:
            out = self.conn_emr.list_clusters(cluster_states=ALL_CLUSTER_STATES)
        else:
            out = self.conn_emr.list_clusters(cluster_states=ACTIVE_CLUSTER_STATES)

        if not out or not out.clusters or len(out.clusters) == 0:
            if not silent:
                print 'No clusters found / available for the selected option.'
        else:
            i = 0
            print "---------- Clusters ----------"
            for cluster in out.clusters:
                print '%d:\n\tID:\t%s\n\tName:\t%s\n\tStatus:\t%s\n--------------------------------\n' \
                      % (i, cluster.id, cluster.name, cluster.status.state)
                i += 1

                op = -1
                control = True
                while control:
                    if not all_states:
                        op = raw_input("Please select the cluster id, in order to analyze the instances "
                                       "(if it is not in range, you'll go back to previous menu): ")
                        op = op.strip()

                        try:
                            int(op)
                        except ValueError:
                            control = False
                            print "Invalid selection! You are going back to previous menu!"
                        else:
                            if int(op) > - 1 and int(op) < i:
                                # select the group of instances of the selected cluster
                                instances = self.conn_emr.list_instance_groups(out.clusters[int(op)].id)

                                master_count = 0
                                core_count = 0
                                task_count = 0
                                for instance in instances.instancegroups:
                                    if instance.instancegrouptype in "MASTER":
                                        master_count += 1
                                    elif instance.instancegrouptype in "CORE":
                                        core_count += 1
                                    elif instance.instancegrouptype in "TASK":
                                        task_count += 1

                                print "\nReport for instances type of %s cluster" % out.clusters[int(op)].name
                                print "%d MASTER" % master_count
                                print "%d CORE" % core_count
                                print "%d TASK" % task_count
                                control = False

        return out.clusters


if __name__ == '__main__':
    from boto.emr.connection import EmrConnection
    # input_select_cluster(EmrConnection())
    #cluster_menu(EmrConnection())
    Cluster().menu()
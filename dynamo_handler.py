from sys import exc_info, stderr

from boto.dynamodb2.fields import HashKey, RangeKey
from boto.dynamodb2.table import Table
from boto.dynamodb2.exceptions import JSONResponseError
from boto.dynamodb2.layer1 import DynamoDBConnection


# This class aims to encapsulate the operations with Amazon NoSQL Engine: Dynamo DB v2
# Docs:
#       * http://boto.readthedocs.org/en/latest/dynamodb2_tut.html
#       * http://boto.readthedocs.org/en/latest/ref/dynamodb2.html
class DynamoHandler:
    CREATE_TABLE_IF_MISSING = True
    MAP_REDUCE_PROFILE_TABLE = 'map_reduce_profiles'
    MAP_REDUCE_PROFILE_SCHEMA = [HashKey('profile_name')]
    MAP_REDUCE_PROFILE_THROUGHPUT = {'read': 1, 'write': 1}

    def __init__(self, region='us-east-1'):
        #self.connection = connect_to_region(region) # bugs the scan list
        self.connection = DynamoDBConnection()

        self.profiles_table = Table(DynamoHandler.MAP_REDUCE_PROFILE_TABLE,
                                    schema=DynamoHandler.MAP_REDUCE_PROFILE_SCHEMA,
                                    throughput=DynamoHandler.MAP_REDUCE_PROFILE_THROUGHPUT,
                                    connection=self.connection)

        if DynamoHandler.CREATE_TABLE_IF_MISSING:
            try:
                self.profiles_table.describe()
            except JSONResponseError:
                if exc_info()[1].error_code == 'ResourceNotFoundException':
                    self.configure_first_run()

    def get_all_profile_names(self):
        ret = None
        profiles = self.connection.scan(table_name=DynamoHandler.MAP_REDUCE_PROFILE_TABLE,
                                        attributes_to_get=['profile_name'])

        if profiles and 'Items' in profiles and len(profiles['Items']) > 0:
            ret = profiles['Items']

        if ret:
            return [elem['profile_name']['S'] for elem in ret]
        else:
            return None

    def list_input_profile(self):
        ret = None
        profiles = self.get_all_profile_names()

        while not ret:
            i = 1

            for elem in profiles:
                print '\t%d.\t%s' % (i, elem)
                i += 1

            try:
                option = int(raw_input("\t0.\tMain Menu\nPlease chose a profile: "))

                if option == 0:
                    break
                elif 0 < option <= len(profiles):
                    ret = self.get_profile_data(elem)
                else:
                    print >> stderr, 'Invalid Option!'

            except ValueError:
                print >> stderr, 'Invalid option'

        return ret

    def get_profile_data(self, profile_name):
        return self.profiles_table.get_item(profile_name=profile_name)._data

    # Todo check repeated
    def create_profile(self, profile_name, sample_path, mapper_file_path, reducer_file_path, combiner_file_path=None):
        if combiner_file_path:
            self.profiles_table.put_item(data={'profile_name': profile_name,
                                               'sample_path': sample_path,
                                               'mapper_file_path': mapper_file_path,
                                               'reducer_file_path': reducer_file_path,
                                               'combiner_file_path': combiner_file_path})
        else:
            self.profiles_table.put_item(data={'profile_name': profile_name,
                                               'sample_path': sample_path,
                                               'mapper_file_path': mapper_file_path,
                                               'reducer_file_path': reducer_file_path})

    def configure_first_run(self):
        print 'Creating new table to hold the Map Reduce Profiles'
        self.profiles_table = Table.create(DynamoHandler.MAP_REDUCE_PROFILE_TABLE,
                                           schema=DynamoHandler.MAP_REDUCE_PROFILE_SCHEMA,
                                           throughput=DynamoHandler.MAP_REDUCE_PROFILE_THROUGHPUT,
                                           connection=self.connection)
        # Word Count profile
        base_path = 's3://eng-serv-teste3/'
        handler.create_profile('Word Count',
                               base_path + 'input/Word_Count',
                               base_path + 'scripts/WordCountMapper.py',
                               base_path + 'scripts/WordCountReducer2.py')


if __name__ == '__main__':
    handler = DynamoHandler()
    #print handler.get_all_profile_names()
    print handler.get_profile_data('Word Count')
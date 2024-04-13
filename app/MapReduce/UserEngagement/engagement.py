# Import necessary packages
import json
from mrjob.job import MRJob
from mrjob.step import MRStep
import happybase

# Define class
class Engagement(MRJob):

    # +++++++ Mapper ++++++++++
    def mapper(self, _, value):
        try:
            line = json.loads(value)

            for data in line:
                favourites = data['favourites_count']
                reblogs = data['reblogs_count']
                followers = data['account']['followers_count']
                username = data['account']['username']
                if followers > 0:
                    yield username, (favourites + reblogs) / followers
                else :
                    yield username, 0
        except:
            yield 'error', 'errorr'

    # Define a combiner to speed up
    def combiner(self, key, values):
        yield key, sum(values)

    # Define a reducer 
    def reducer(self, key, values):
        yield None, (sum(values), key)

    # Sort the reduced values
    def reduce_sorter(self, key, values):
        for count, key in sorted(values):
            yield key, count

    # Define steps
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer
            ),
            MRStep(reducer=self.reduce_sorter)
        ]

if __name__ == '__main__':

    # Run the MRJob script
    mr_job = Engagement()

    with mr_job.make_runner() as runner:
        # Run the job
        runner.run()

        # Connect to HBase
        connection = happybase.Connection('hbase-master')

        # Create a table if it does not exist
        if b'User' not in connection.tables():
            connection.create_table(
                'User',
                {'cf': dict()}
            )

        table = connection.table('User')

        # Insert the data into HBase
        for key, value in mr_job.parse_output(runner.cat_output()):
            table.put(key.encode(), {'cf:Engagement': str(value).encode()})
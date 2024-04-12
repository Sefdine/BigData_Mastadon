# Import necessary packages
import json
from datetime import datetime
from mrjob.job import MRJob
from mrjob.step import MRStep
import happybase as hb

# Define class
class UserGrowthOverTime(MRJob):
    
    # Define the mapper
    def mapper(self, _, value):

        try:

            line = json.loads(value)

            for data in line:
                date_string = data['account']['created_at']

                # Define a format for the input date string
                date_format = "%Y-%m-%dT%H:%M:%S.%fZ"

                # Parse the date string and create a datetime object
                user_created_at = datetime.strptime(date_string, date_format)

                yield user_created_at.year, 1
        except:
            yield 'error', 'error'

    # Define a combiner to speed up the process
    def combiner(self, key, values):
        yield key, sum(values)

    # Reduce values
    def reducer(self, key, values):
        yield None, (sum(values), key)

    # Sort the values
    def reduce_sorter(self, _, values):
        for count, key in sorted(values, key=lambda x: x[1]):
            yield key, count
    
    # Define steps
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.reduce_sorter)
        ]
    
if __name__ == '__main__':

    # Run the MRJob script
    mr_job = UserGrowthOverTime()

    with mr_job.make_runner() as runner:
        # Run the job
        runner.run()

        # Connect to the hbase
        connection = hb.Connection('localhost')

        # Create a table if does not exists
        if b'UserGrowthOverTime' not in connection.tables():
            connection.create_table(
                'UserGrowthOverTime',
                {'cf': dict()}
            )

        # Connect to the table
        table = connection.table('UserGrowthOverTime')

        # Insert the data into Hbase
        for key, value in mr_job.parse_output(runner.cat_output()):
            table.put(str(key).encode(), {'cf:numberOfUsers': str(value).encode()})

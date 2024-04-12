# Import necessary packages
import json
from mrjob.job import MRJob
from mrjob.job import MRStep
import happybase as hb

# Define class
class SharedWebsite(MRJob):

    # Define mapper
    def mapper(self, _, value):

        try:
            line = json.loads(value)

            for data in line:
                url = data['url']
                base_url = url.split('/')[2]

                yield base_url, 1
        except:
            yield 'error', 'error'

    # Define a combiner to speed up the process
    def combiner(self, key, values):
        yield key, sum(values)
    
    # Define a reducer
    def reducer(self, key, values):
        yield None, (sum(values), key)

    # Sort values
    def reduce_sorter(self, key, values):
        for count, key in sorted(values):
            yield key, count

    # Define steps
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.reduce_sorter)
        ]

if __name__ == '__main__':
    mr_job = SharedWebsite()

    with mr_job.make_runner() as runner:

        # Run the job
        runner.run()

        # Connect to Hbase
        connection = hb.Connection('localhost')

        # Create the table if does not exists
        if b'SharedWebsite' not in connection.tables():
            connection.create_table(
                'SharedWebsite',
                {'cf': dict()}
            )

        # Connect to the table
        table = connection.table('SharedWebsite')

        # Insert the data into Hbase
        for key, value in mr_job.parse_output(runner.cat_output()):
            table.put(key.encode(), {'cf:count': str(value).encode()})
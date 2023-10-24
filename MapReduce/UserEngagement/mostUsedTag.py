# Import necessary packages
import json
from mrjob.job import MRJob
from mrjob.step import MRStep
import happybase

# Define the class
class MostUsedTag(MRJob):

    # Map the dataset and preapare key-value
    def mapper(self, _, value):
        try:
            line = json.loads(value)

            for data in line:
                
                # Get username 
                username = data['account']['username']
                # Get tags
                tags = data['tags']
                for tag in tags:
                    yield tag['name'], username
        except:
            yield 'error', 'errorr'

    # Reduce with the len of users and print unique users
    def reducer_count_tags(self, key, values):
        list_values = list(values)
        yield None, (len(list_values), list(set(list_values)), key)

    # Sort the output result
    def reducer_sort_tags(self, key, values):
        for count, user, key in sorted(values):
            yield key, (count, user)


    # Define steps
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_count_tags),
            MRStep(reducer=self.reducer_sort_tags)
        ]
    
if __name__ == '__main__':

    # Run the MRJob script
    mr_job = MostUsedTag()

    with mr_job.make_runner() as runner:
        # Run the job
        runner.run()

        # Connect to HBase
        connection = happybase.Connection('localhost')

        # Create a table if it does not exist
        if b'Tags' not in connection.tables():
            connection.create_table(
                'Tags',
                {'cf': dict()}
            )

        table = connection.table('Tags')

        # Insert the data into HBase
        for key, value in mr_job.parse_output(runner.cat_output()):
            table.put(key.encode(), {'cf:AttachedUsers': str(value).encode()})
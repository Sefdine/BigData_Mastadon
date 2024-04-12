# Import necessary packages
import json
from mrjob.job import MRJob
from mrjob.step import MRStep
import happybase as hb

# Define class
class PostMultimediaCount(MRJob):

    # Define mapper
    def mapper(self, _, value):
        try:
            data = json.loads(value)
            for line in data:
                media_attachments = line['media_attachments']
                if len(media_attachments) > 0:
                    yield 'mediaAttached', 1
        except:
            yield 'error', 'error'

    # Define the reducer
    def reducer(self, key, values):
        yield key, sum(values)

    # Define steps
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

if __name__ == '__main__':
    mr_job = PostMultimediaCount()

    with mr_job.make_runner() as runner:
        
        # Run the job
        runner.run()

        # Connect to Hbase
        connection = hb.Connection('localhost')

        # Create the table if does not exists
        if b'PostMultimediaCount' not in connection.tables():
            connection.create_table(
                'PostMultimediaCount',
                {'cf': dict()}
            )
        
        # Connect to the table
        table = connection.table('PostMultimediaCount')

        # Insert the data into Hbase
        for key, value in mr_job.parse_output(runner.cat_output()):
            table.put(key.encode(), {'cf:AttachedMediaCount': str(value).encode()})
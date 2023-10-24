# Import necessary packages
import json
from mrjob.job import MRJob
from mrjob.step import MRStep
import happybase as hb

# Define class
class LanguagePostCategorize(MRJob):

    # Define mapper
    def mapper(self, _, value):
        try:
            line = json.loads(value)

            for data in line:
                language = data['language']
                post_id = data['id']

                if not language:
                    language = 'Unknown Language'

                yield language, post_id
        except:
            yield 'error', 'error'

    # Define reducer
    def reducer_counter(self, key, values):
        list_values = list(values)
        yield None, (len(list_values), list(set(list_values)), key)

    # Sort values
    def reduce_sorter(self, key, values):
        for count, posts, key in sorted(values):
            yield key, (count, posts)


    # Define steps
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper, 
                reducer=self.reducer_counter
            ),
            MRStep(reducer=self.reduce_sorter)
        ]

if __name__ == '__main__':
    
    mr_job = LanguagePostCategorize()

    with mr_job.make_runner() as runner:

        # Run the job
        runner.run()

        # Connect to Hbase
        connection = hb.Connection('localhost')

        # Create table if doesn't exists
        if b'LanguagePosts' not in connection.tables():
            connection.create_table(
                'LanguagePosts',
                {'cf': dict()}
            )

        # Connect to the table
        table = connection.table('LanguagePosts')

        # Insert the data into Hbase
        for key, value in mr_job.parse_output(runner.cat_output()):
            count = value[0]
            post_id = value[1]
            table.put(key.encode(), {'cf:count': str(count).encode(), 'cf:post_id':str(post_id).encode()})
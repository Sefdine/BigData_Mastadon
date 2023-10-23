# Import necessary packages
import json
from mrjob.job import MRJob
from mrjob.job import MRStep

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
    SharedWebsite().run()
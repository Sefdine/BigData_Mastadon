# Import necessary packages
import json
from mrjob.job import MRJob
from mrjob.step import MRStep

# Define class
class Followers(MRJob):

    # Define the mapper
    def mapper(self, _, value):
        try:
            line = json.loads(value)

            for data in line:
                followers = data['account']['followers_count']
                username = data['account']['username']

                yield "Followers:"+username, followers
        except:
            yield 'error', 'errorr'

    # Define a combiner to speed up the process
    def combiner(self, key, values):
        yield key, sum(values)

    # Define the reducer
    def reducer(self, key, values):
        yield None, (sum(values), key)

    # Define a sorter
    def reduce_sorter(self, key, values):
        for count, key in sorted(values):
            yield key, count

    # Steps
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
    Followers.run()
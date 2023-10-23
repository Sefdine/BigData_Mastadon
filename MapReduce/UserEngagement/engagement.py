# Import necessary packages
import json
from mrjob.job import MRJob
from mrjob.step import MRStep

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
                    yield "engagement_rate "+username, (favourites + reblogs) / followers
                else :
                    yield "engagement_rate "+username, 0
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
    Engagement.run()
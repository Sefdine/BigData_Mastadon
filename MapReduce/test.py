from mrjob.job import MRJob
from mrjob.step import MRStep

# Import necessary packages
import json
import re

class WordCounter(MRJob):

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

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            )
        ]
    
if __name__ == '__main__':
    WordCounter.run()
# Import necessary packages
import json
from datetime import datetime
from mrjob.job import MRJob
from mrjob.step import MRStep

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
    UserGrowthOverTime().run()
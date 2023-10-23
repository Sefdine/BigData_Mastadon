# Import necessary packages
import json
from mrjob.job import MRJob
from mrjob.step import MRStep

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
    MostUsedTag.run()
# Import necessary packages
import json
from mrjob.job import MRJob
from mrjob.step import MRStep

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
    LanguagePostCategorize().run()
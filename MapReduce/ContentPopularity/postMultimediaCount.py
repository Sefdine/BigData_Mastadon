# Import necessary packages
import json
from mrjob.job import MRJob
from mrjob.step import MRStep

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
    PostMultimediaCount().run()
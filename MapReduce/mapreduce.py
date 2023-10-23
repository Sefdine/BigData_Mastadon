from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class SocialMediaAnalysis(MRJob):

    def mapper(self, _, line):
        try:
            data = json.loads(line)
            # Implement the mapper logic based on the metrics you want to analyze
            yield "engagement_rate", (data['favourites_count'] + data['reblogs_count']) / data['replies_count']
            yield "user_followers", data['account']['followers_count']
            yield "url_count", len(data.get('urls', []))
            yield "emoji_count", len(data.get('emojis', []))
        except json.JSONDecodeError as e:
            # Handle the case where the JSON is not formatted correctly
            yield "error", str(e)

    def reducer(self, key, values):
        if key == "engagement_rate":
            total = 0
            count = 0
            for value in values:
                total += value
                count += 1
            yield key, total / count

        elif key == "user_followers" or key == "url_count" or key == "emoji_count":
            yield key, sum(values)

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            )
        ]

if __name__ == '__main__':
    SocialMediaAnalysis().run()
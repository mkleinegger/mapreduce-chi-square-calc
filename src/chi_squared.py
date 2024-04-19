from pathlib import Path
from typing import Generator, Union
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import json


class ChiSquaredJob(MRJob):


    def configure_args(self):
        super(ChiSquaredJob, self).configure_args()

        self.FILES = [str(Path(__file__).parent.parent / 'data' / 'stopwords.txt')]
        self.add_file_arg('--stopwords', default='stopwords.txt')
        self.add_passthru_arg('-k', type=int, default=75)


    def init_stopwords(self):
        with open(self.options.stopwords, 'r') as file:
            self.stopwords = set(file.read().splitlines())


    def mapper_preprocessing(self, _, line: str):
        data = json.loads(line)
        category = data['category']

        # lower text
        # tokenises each line by using whitespaces, tabs, digits, and the characters ()[]{}.!?,;:+=-_"'`~#@&*%€$§\/ as delimiters
        tokens = re.split('[^a-zA-Z<>^|]+', data['reviewText'].lower())

        # remove stopwords
        tokens = filter(lambda token: len(token) > 1 and (token not in self.stopwords), tokens)

        # remove duplicates
        tokens = set(tokens)

        # count all documents in category
        yield (category, None), 1

        # count all documents in category containing token
        for token in tokens:
            yield (category, token), 1


    def combiner_count(self, key: Union[tuple[str, str], None], values: Generator[list[int], None, None]):
        yield key, sum(values)


    def reducer_count(self, key: Union[tuple[str, str], None], values: Generator[list[int], None, None]):
        category, token = key
        yield token, (category, sum(values))


    def reducer_token_sum(self, token: str, values: Generator[tuple[str, int], None, None]):
        # collecting dictionary of all tokens
        if token is not None:
            yield None, token
    
        counts = {category: count for category, count in values}
        n_t = sum(counts.values())

        for category, count in counts.items():
            yield category, (token, count, n_t)


    def reducer_chi_squared(self, category: str, values: Generator[tuple[str, int, int], None, None]):
        # collecting dictionary of all tokens
        if category is None:
            yield None, sorted(values)
            return

        counts = {token: (count, n_t) for token, count, n_t in values}
        n_c, n = counts.pop(None)
        result = []

        for token, (a, n_t) in counts.items():
            b = n_t - a
            c = n_c - a
            d = n - a - b - c

            chi_squared = n * ((a * d - b * c) ** 2) / ((a + b) * (a + c) * (b + d) * (c + d))
            result.append((chi_squared, token))

        yield category, sorted(result, reverse=True)[:self.options.k]


    def steps(self) -> list[MRStep]:
        return [
            MRStep(mapper_init=self.init_stopwords,
                   mapper=self.mapper_preprocessing,
                   combiner=self.combiner_count,
                   reducer=self.reducer_count),
            MRStep(reducer=self.reducer_token_sum),
            MRStep(reducer=self.reducer_chi_squared),
        ]


if __name__ == '__main__':
    ChiSquaredJob.run()
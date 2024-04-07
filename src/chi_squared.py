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

        # tokenises each line by using whitespaces, tabs, digits, and the characters ()[]{}.!?,;:+=-_"'`~#@&*%€$§\/ as delimiters 
        tokens = re.split('[^a-zA-Z<>^|]+', data['reviewText'])

        # case fold
        tokens = map(str.lower, tokens)

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
        counts = {category: count for category, count in values}
        token_sum = sum(counts.values())

        for category, count in counts.items():
            yield category, (token, count, token_sum)


    def reducer_chi_squared(self, category: str, values: Generator[tuple[str, int, int], None, None]):
        counts = {token: (count, token_sum) for token, count, token_sum in values}
        category_sum, n = counts.pop(None)

        for token, (a, token_sum) in counts.items():
            b = token_sum - a
            c = category_sum - a
            d = n - a - b - c

            chi_squared = n * ((a * d - b * c) ** 2) / ((a + b) * (a + c) * (b + d) * (c + d))

            yield category, (chi_squared, token)


    def combiner_top_k(self, key: str, data: list[any]):
        top_k = sorted(data, reverse=True)[:self.options.k]

        for value in top_k:
            yield key, value


    def reducer_top_k(self, key: str, data: list[any]):
        top_k = sorted(data, reverse=True)[:self.options.k]

        yield key, tuple(top_k)


    def steps(self) -> list[MRStep]:
        return [
            MRStep(mapper_init=self.init_stopwords,
                   mapper=self.mapper_preprocessing,
                   combiner=self.combiner_count,
                   reducer=self.reducer_count),
            MRStep(reducer=self.reducer_token_sum),
            MRStep(reducer=self.reducer_chi_squared),
            MRStep(combiner=self.combiner_top_k,
                   reducer=self.reducer_top_k),
        ]


if __name__ == '__main__':
    ChiSquaredJob.run()
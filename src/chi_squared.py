from typing import Generator
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import json


class ChiSquaredJob(MRJob):


    def configure_args(self):
        super(ChiSquaredJob, self).configure_args()

        self.FILES = ['../data/stopwords.txt']
        self.add_file_arg('--stopwords')
        self.add_passthru_arg('-k', type=int, default=75)


    def load_args(self, args):
        super(ChiSquaredJob, self).load_args(args)

        self.stopwords_file = self.options.stopwords if self.options.stopwords else 'stopwords.txt'
        self.k = self.options.k


    def init_stopwords(self):
        with open(self.stopwords_file, 'r') as file:
            self.stopwords = set(file.read().splitlines())


    def mapper_preprocessing(self, _, line: str):
        data = json.loads(line)

        document = (data['reviewerID'], data['asin'])
        category = data['category']

        # tokenises each line by using whitespaces, tabs, digits, and the characters ()[]{}.!?,;:+=-_"'`~#@&*%€$§\/ as delimiters 
        tokens = re.split('[^a-zA-Z<>^|]+', data['reviewText'])

        # case fold
        tokens = map(str.lower, tokens)

        # remove stopwords
        tokens = filter(lambda token: len(token) > 1 and (token not in self.stopwords), tokens)

        for token in tokens:
            yield (document, category, token), None              # count all documents in category containing token
            yield (document, category, None), None           # count all documents in category


    def combiner_duplicate_elimination(self, key: tuple[str, str, str], values: Generator[any, None, None]):
        yield key, None


    def reducer_duplicate_elimination(self, key: tuple[str, str, str], values: Generator[any, None, None]):
        _, category, token = key
        yield (category, token), 1


    def combiner_count(self, key: tuple[str, str], values: Generator[list[int], None, None]):
        yield key, sum(values)


    def reducer_count(self, key: tuple[str, str], values: Generator[list[int], None, None]):
        category, token = key

        if token is None:
            yield None, (category, sum(values))
        else:
            yield category, (token, sum(values))


    def reducer_total_sum(self, category: str | None, values: Generator[tuple[str, int], None, None]):
        if category is not None:
            for value in values:
                yield category, value
            return
        
        categories, counts = zip(*values)
        total_count = sum(counts)
        
        for category, count in zip(categories, counts):
            yield category, (None, (count, total_count))


    def reducer_distribute_sums(self, category: str, values: Generator[tuple[str, int], None, None]):
        counts = {token: count for token, count in values}
        category_sum, total_sum = counts.pop(None)

        for token, count in counts.items():
            yield token, (category, count, category_sum, total_sum)


    def reducer_chi_squared(self, token: str, values: Generator[tuple[str, int, int], None, None]):
        categories, counts, category_sums, total_sums = zip(*values)
        token_sum = sum(counts)

        for category, a, category_sum, n in zip(categories, counts, category_sums, total_sums):
            b = token_sum - a
            c = category_sum - a
            d = n - a - b - c

            chi_squared = n * ((a * d - b * c) ** 2) / ((a + b) * (a + c) * (b + d) * (c + d))

            yield category, (chi_squared, token)


    def combiner_top_k(self, key: str, data: list[any]):
        top_k = sorted(data, reverse=True)[:self.k]

        for value in top_k:
            yield key, value


    def reducer_top_k(self, key: str, data: list[any]):
        top_k = sorted(data, reverse=True)[:self.k]

        yield key, tuple(top_k)


    def steps(self) -> list[MRStep]:
        return [
            MRStep(mapper_init=self.init_stopwords, mapper=self.mapper_preprocessing),
            MRStep(combiner=self.combiner_duplicate_elimination, reducer=self.reducer_duplicate_elimination),
            MRStep(combiner=self.combiner_count, reducer=self.reducer_count),
            MRStep(reducer=self.reducer_total_sum),
            MRStep(reducer=self.reducer_distribute_sums),
            MRStep(reducer=self.reducer_chi_squared),
            MRStep(combiner=self.combiner_top_k, reducer=self.reducer_top_k),
        ]


if __name__ == '__main__':
    ChiSquaredJob.run()
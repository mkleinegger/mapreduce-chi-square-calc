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

        key = (data['reviewerID'], data['asin'])
        category = data['category']

        # tokenises each line by using whitespaces, tabs, digits, and the characters ()[]{}.!?,;:+=-_"'`~#@&*%€$§\/ as delimiters 
        tokens = re.split('[^a-zA-Z<>^|]+', data['reviewText'])

        # case fold
        tokens = map(str.lower, tokens)

        # remove stopwords
        tokens = filter(lambda token: len(token) > 1 and (token not in self.stopwords), tokens)

        for token in tokens:
            yield key, (category, token)


    def mapper_count(self, key: tuple[str, str], data: tuple[str, str]):
        category, token = data

        yield (None, None), ([key], [(category, token)])    # count all documents
        yield (category, None), ([key], [token])            # count all documents in category
        yield (None, token), ([key], [category])            # count all documents containing token
        yield (category, token), ([key], [])                # count all documents in category containing token


    def _eliminate_duplicates(self, cat_tok: tuple[str, str], values: Generator[tuple, None, None]):
        keys, cat_toks = set(), set()

        if cat_tok == [None, None]:
            for key, cat_tok in values:
                keys.update(map(tuple, key))
                cat_toks.update(map(tuple, cat_tok))
        else:
            for key, cat_tok in values:
                keys.update(map(tuple, key))
                cat_toks.update(cat_tok)

        return keys, cat_toks
    

    def combiner_count(self, cat_tok: tuple[str, str], values: Generator[tuple, None, None]):
        keys, cat_toks = self._eliminate_duplicates(cat_tok, values)

        yield cat_tok, (tuple(keys), tuple(cat_toks))


    def reducer_count(self, cat_tok: tuple[str, str], values: Generator[tuple, None, None]):
        keys, cat_toks = self._eliminate_duplicates(cat_tok, values)
        category, token = cat_tok
        n = len(keys)
        
        if (category is None) and (token is None):
            for (category, token) in cat_toks:
                yield (category, token), ('n', n)
        elif category is None:
            for category in cat_toks:
                yield (category, token), ('t', n)
        elif token is None:
            for token in cat_toks:
                yield (category, token), ('c', n)
        else:
            yield cat_tok, ('ct', n)


    def reducer_chi_squared(self, cat_tok: tuple[str, str], values: Generator[tuple[str, int], None, None]):
        category, token = cat_tok
        counts = {key: value for key, value in values}

        n = counts['n']
        n_c_t = counts.get('ct', 0)
        n_t = counts.get('t', 0)
        n_c = counts.get('c', 0)
        n_c_nt = n_c - n_c_t
        n_nt = n - n_t
        n_nc_t = n_t - n_c_t
        n_nc_nt = n_nt - n_c_nt

        chi_squared = n * ((n_c_t * n_nc_nt - n_nc_t * n_c_nt) ** 2) / ((n_c_t + n_nc_t) * (n_c_t + n_c_nt) * (n_nc_t + n_nc_nt) * (n_c_nt + n_nc_nt))

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
            MRStep(mapper=self.mapper_count, combiner=self.combiner_count, reducer=self.reducer_count),
            MRStep(reducer=self.reducer_chi_squared),
            MRStep(combiner=self.combiner_top_k, reducer=self.reducer_top_k),
        ]


if __name__ == '__main__':
    ChiSquaredJob.run()
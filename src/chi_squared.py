from itertools import chain
from typing import Generator
from mrjob.step import MRStep
from subtask import Subtask

class ChiSquared(Subtask):

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


    def steps(self) -> list[MRStep]:
        return [
            MRStep(mapper=self.mapper_count, combiner=self.combiner_count, reducer=self.reducer_count),
            MRStep(reducer=self.reducer_chi_squared),
        ]
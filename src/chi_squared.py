from pathlib import Path
from typing import Generator, Union
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import json


class ChiSquared(MRJob):
    """
    This class implements a MapReduce job that calculates the chi-squared values
    for each token in each category of Amazon reviews and outputs the top k
    tokens with the highest chi-squared values for each category.
    """


    def configure_args(self):
        """
        Configures the command-line arguments for the job.

        Arguments:
            -k: The number of tokens with top chi-squared values to output for each category.
            --stopwords: The path to the file containing the stopwords.
        """

        super(ChiSquared, self).configure_args()

        self.FILES = [str(Path(__file__).parent.parent / 'data' / 'stopwords.txt')]
        self.add_file_arg('--stopwords', default='stopwords.txt')
        self.add_passthru_arg('-k', type=int, default=75)


    def init_stopwords(self):
        """
        Initialises the stopwords set from the file specified in the
        command-line arguments before the preprocessing mapper is called.
        """

        with open(self.options.stopwords, 'r') as file:
            self.stopwords = set(file.read().splitlines())


    def mapper_preprocessing(self, _, line: str):
        """
        Maps the lines of the input file, each containing one review in JSON format,
        to the corresponding category and a set of tokens contained in the review.

        The review text is tokenised by converting it to lowercase, splitting it
        and removing short words, stopwords, and duplicates.

        Input:
            _: The key of the input line. (Ignored)
            line: The value of the input line, containing a review in JSON format.

        Output:
            (category, None), 1: For every review, so for every call of the mapper
                the value 1 is emitted with the token in the key being set to None
                in order to count the number of reviews in each category.
            (category, token), 1: For every token in the review, the value 1 is emitted
                to count the number of reviews in each category containing the token.
        """

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
        """
        Combiner function that sums up the count values for each category and token. combination.

        Input:
            key: The key of the input values, containing the category and token.
            values: The values of the input key, containing the count values produced by the mapper.

        Output:
            key, sum(values): The key of the input values and the sum of the count values. 
        """

        yield key, sum(values)


    def reducer_count(self, key: Union[tuple[str, str], None], values: Generator[list[int], None, None]):
        """
        Reducer function that sums up the count values for each category and token.

        Input:
            key: The key of the input values, containing the category and token.
            values: The values of the input key, containing the count values produced by the combiner.

        Output:
            token, (category, sum(values)): Emits the token as the new key and a
                tuple containing the category and the count of documents for the
                category and token combination as the new value.
        """

        category, token = key
        yield token, (category, sum(values))


    def reducer_token_sum(self, token: str, values: Generator[tuple[str, int], None, None]):
        """
        Reducer function that sums up the document counts for each token. Since
        reviews can only belong to one category, the sum of the document counts
        over all categories does not contain duplicated counts.

        In the special case of the token being None, the reducer calculates the
        total number of documents in the dataset.

        Input:
            token: The token of the input values.
            values: List containing the category and the document count for the
                category and token combination.

        Output:
            category, (token, count, n_t): Emits for each category contained in
                the values the category as the new key and a tuple containing
                the token, the count of documents containing the token in the
                category, and the total number of documents containing the token
                as the new value.
                For the special case of the token being None, "count" contains
                the total number of documents in the category and "n_t" contains
                the total number of documents in the dataset.
        """

        counts = {category: count for category, count in values}
        n_t = sum(counts.values())

        for category, count in counts.items():
            yield category, (token, count, n_t)


    def reducer_chi_squared(self, category: str, values: Generator[tuple[str, int, int], None, None]):
        """
        Reducer function that calculates the chi-squared values for each token 
        in the category and outputs the top k tokens with the highest chi-squared
        values.

        Input:
            category: The category of the input values.
            values: List containing the token, the count of documents containing
                the token in the category, and the total number of documents
                containing the token.
                For the special case of the token being None, it contains the
                total number of documents in the category and the total number
                of documents in the dataset.

        Output:
            category, [(chi_squared, token)]: Emits the category as the key and a
                list of tuples containing the chi-squared value and the token as
                the value. The list is sorted by the chi-squared values in
                descending order and the tokens in ascending order and contains
                the top k tokens with the highest chi-squared values.
        """

        # dictionary of tokens with their counts and total number of documents
        counts = {token: (count, n_t) for token, count, n_t in values}
        
        # total number of documents in category and dataset
        n_c, n = counts.pop(None)
        
        result = []

        for token, (a, n_t) in counts.items():
            b = n_t - a
            c = n_c - a
            d = n - a - b - c

            chi_squared = n * ((a * d - b * c) ** 2) / ((a + b) * (a + c) * (b + d) * (c + d))
            result.append((chi_squared, token))

        yield category, sorted(result, key=lambda x: (-x[0], x[1]))[:self.options.k]


    def steps(self) -> list[MRStep]:
        """
        Defines the steps of the MapReduce job.
        """

        return [
            MRStep(mapper_init=self.init_stopwords,
                   mapper=self.mapper_preprocessing,
                   combiner=self.combiner_count,
                   reducer=self.reducer_count),
            MRStep(reducer=self.reducer_token_sum),
            MRStep(reducer=self.reducer_chi_squared),
        ]


if __name__ == '__main__':
    ChiSquared.run()
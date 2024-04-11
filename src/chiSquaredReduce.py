from pathlib import Path
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import json

class ChiSquaredProcessor(MRJob):

    def configure_args(self):
        super(ChiSquaredProcessor, self).configure_args()

        self.FILES = [str(Path(__file__).parent.parent / 'data' / 'stopwords.txt')]
        self.add_file_arg('--stopwords', default='stopwords.txt')
        self.add_passthru_arg('-k', type=int, default=75)


    def init_stopwords(self):
        with open(self.options.stopwords, 'r') as file:
            self.stopwords = set(file.read().splitlines())

    def mapper_preprocessing(self, _, line):
        """ 
        This mapper returns all terms per category for each document occurence
        Returns a key value pair of: (category, word), 1
        """

        data = json.loads(line)
        category = data.get('category', '')
        reviewText = data.get('reviewText', '')
        word_list = re.split('[^a-zA-Z<>^|]+', reviewText.lower())

        word_set = set([word for word in word_list if word not in self.stopwords and word.strip() != '' and len(word) > 1])

        yield (category, None), 1
        for term in word_set:
            yield (category, term), 1

    def reducer_count_terms(self, category_term, counts):
        """ 
        This reducer sums the occurences of each term per category
        Returns a key value pair of: (category, term), number
        """
        category, term = category_term
        sum_counts = sum(counts)
        yield term, (category, sum_counts)

    def reducer_count_terms_over_categories(self, term, category_count):
        """
        This reducer returns the number of occurences of a term in all categories and the number of occurences of all terms
        Returns a key value pair of: term, [(category, count_term, number_of_occurences)]
        """

        category_count = list(category_count)
        number_of_occurences = sum([count for _, count in category_count])
        for category, count_term in category_count:
            yield category, (term, count_term, number_of_occurences)

    def reducer_calc_chi_squared(self, category, list_category_count): 
        """
        This reducer calculates the chi squared value for each term per category.
        Returns a key value pair of: (category, term), chi_squared
        """
        if category is None:
            yield None, list(set(list_category_count))
            return

        map_category_count = { term: (count_term, number_of_occurences) for term, count_term, number_of_occurences in list_category_count}
        category_count, N = map_category_count.pop(None)

        results = []
        for term, count in map_category_count.items():
            count_term, number_of_occurences = count
            A = count_term
            B = number_of_occurences - count_term
            C = category_count - count_term
            D = N - category_count - B
            results.append((term, N * (A*D - B*C)**2 / ((A+B)*(A+C)*(B+D)*(C+D))))
            
        yield category, sorted(results, key=lambda x: x[1], reverse=True)[:self.options.k]

    def steps(self):
        return [
            MRStep(
                mapper_init = self.init_stopwords,
                mapper   = self.mapper_preprocessing,
                reducer  = self.reducer_count_terms
            ),
            MRStep(reducer  = self.reducer_count_terms_over_categories),
            MRStep(reducer  = self.reducer_calc_chi_squared)
        ]
   
if __name__ == '__main__':
    ChiSquaredProcessor.run()

from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import json

stopwords = set()
category_counts = {}

class CountingProcessor(MRJob):
    def mapper(self, _, line):
        """ 
        This mapper returns for each review of a category 1
        Returns a key value pair of: category, 1
        """

        data = json.loads(line)
        category = data.get('category', '')
        yield category, 1

    def reducer(self, category, counts):
        """
        This reducer sums the number of reviews per category
        Returns a key value pair of: category, number of reviews
        """
        yield category, sum(counts)

    def steps(self):
        return [
            MRStep(
                mapper   = self.mapper,
                reducer  = self.reducer
            )
        ]

class ChiSquaredProcessor(MRJob):

    def mapper_1(self, _, line):
        """ 
        This mapper returns all terms per category for each document occurence
        Returns a key value pair of: (category, word), 1
        """

        data = json.loads(line)
        category = data.get('category', '')
        reviewText = data.get('reviewText', '')
        word_list = re.split('[^a-zA-Z<>^|]+', reviewText.lower())

        word_set = set([word for word in word_list if word not in stopwords and word.strip() != '' and len(word) > 1])
        for word in word_set:
            yield (category, word), 1

    def reducer_1(self, category_term, compromised_reviewText):
        """ 
        This reducer sums the occurences of each term per category
        Returns a key value pair of: (category, term), number
        """

        yield category_term, sum(compromised_reviewText)

    def mapper_2(self, category_term, count_term):
        """
        This mapper returns all categories which a term is occuring in and the respective number of occurences per category
        Returns a key value pair of: term, (category, count_term)
        """

        category, term = category_term
        yield term, (category, count_term)

    def reducer_2(self, term, category_count):
        """
        This reducer returns the number of occurences of a term in all categories and the number of occurences of all terms
        Returns a key value pair of: term, [(category, count_term, number_of_occurences)]
        """

        all_categories_count = list(category_count)
        number_of_occurences = sum([count for _, count in all_categories_count])
        yield term, [(category, count_term, number_of_occurences) for category, count_term in all_categories_count]

    def mapper_3(self, term, list_category_count):
        """
        This mapper groups the occurences of each term per category
        Returns a key value pair of: (category, term), (count_term, number_of_occurences)
        """

        for category_count in list_category_count:
            category, count_term, number_of_occurences = category_count
            yield (category, term), (count_term, number_of_occurences)

    def reducer_3(self, category_term, list_category_count): 
        """
        This reducer calculates the chi squared value for each term per category.
        Returns a key value pair of: (category, term), chi_squared
        """

        number_count_occurence_N = list(list_category_count)
        category, _ = category_term
        for count in number_count_occurence_N:
            count_term, number_of_occurences = count
            A = count_term
            B = number_of_occurences - count_term
            C = category_counts[category] - count_term
            D = category_counts["N"] - category_counts[category] - B
            chi_squared = category_counts["N"] * (A*D - B*C)**2 / ((A+B)*(A+C)*(B+D)*(C+D))
            yield category_term, chi_squared

    def mapper_4(self, category_term, chi_squared):
        """
        This mapper groups the chi squared values for each term by category. Additionally it returns all terms per category.
        Returns a key value pair of: category, (term, chi_squared)
        """

        category, term = category_term
        yield category, (term, chi_squared)
        yield None, term

    def reducer_4(self, category, term_chi_squared):
        """
        This reducer sorts the chi squared values and returns the 75 highest values for each category. If no category is given, all terms are returned.
        Returns a key value pair of: category, [term=chi_squared]
        """

        if category is None:
            all_words = set(term_chi_squared)
            all_words = sorted(all_words)
            yield ', '.join(all_words), ""
        else:
            chi_squared_terms = list(term_chi_squared)
            chi_squared_terms.sort(key=lambda x: x[1], reverse=True)
            yield category, ' '.join(f"{x}:{y}" for x, y in chi_squared_terms[:75])


    def steps(self):
        return [
            MRStep(
                mapper   = self.mapper_1,
                reducer  = self.reducer_1
            ),
            MRStep(
                mapper   = self.mapper_2,
                reducer  = self.reducer_2
            ),
            MRStep(
                mapper   = self.mapper_3,
                reducer  = self.reducer_3
            ),
            MRStep(
                mapper   = self.mapper_4,
                reducer  = self.reducer_4
            )
        ]
   
if __name__ == '__main__':
    myjob = CountingProcessor()
    with myjob.make_runner() as runner:
        runner.run()
        for key, value in myjob.parse_output(runner.cat_output()):           
            category_counts[key] = value
        category_counts["N"] = sum(category_counts.values())

    with open('./data/stopwords.txt', 'r') as f:
        stopwords = set(f.read().splitlines())

    mrjob = ChiSquaredProcessor()
    with mrjob.make_runner() as runner:
        runner.run()
        
        for key, value in mrjob.parse_output(runner.cat_output()):           
            print(key, value, "\n", end='')

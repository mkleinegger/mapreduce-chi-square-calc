from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import json

stopwords = set()

class ChiSquaredProcessor(MRJob):

    def mapper_1(self, _, line):
        """
        This Mapper return the category and the set of words in the reviewText
        Returns a key value pair of: None, (category, word_set)
        """

        data = json.loads(line)
        category = data.get('category', '')
        reviewText = data.get('reviewText', '')
        word_list = re.split('[^a-zA-Z<>^|]+', reviewText.lower())
        word_set = set([word for word in word_list if word not in stopwords and word.strip() != '' and len(word) > 1])
        yield None, (category, list(word_set))

    def reducer_1(self, _, compromised_reviewText):
        """
        This reducer groups the reviews by category and counts the number of all documents
        Returns a key value pair of: category, (reviews, N)
        """

        compromised_reviews = list(compromised_reviewText)
        N = len(compromised_reviews)
        for category, reviews in compromised_reviews:
            yield category, (reviews, N)

    def mapper_2(self, category, reviews_N):
        """
        This mapper returns all reviews for each category. N (the number of documents), is also passed along per category
        Returns a key value pair of: (category, N), reviews
        """

        reviews, N = reviews_N
        yield (category, N), reviews

       
    def reducer_2(self, category_N, reviews):
        """ 
        This reducer counts the number of documents per category and returns for each category the number of documents and all reviews
        Returns a key value pair of: (category, N), (number of documents, reviews)
        """

        all_reviews = list(reviews)
        yield category_N, (len(all_reviews), all_reviews)


    def mapper_3(self, category_N, all_reviews):
        """ 
        This mapper groups the terms per category to allow counting their occurences
        Returns a key value pair of: (category, term), (1, count, N)
        """

        count, reviews = all_reviews
        N = category_N[1]
        category = category_N[0]
        for review in reviews:
            for term in review:
                yield (category, term), (1, count, N)

    def reducer_3(self, category_term, counts): 
        """ 
        This reducer counts the occurences of each term per category
        Returns a key value pair of: (category, term), (number, count, N)
        """

        documents = list(counts)
        count = documents[0][1]
        N = documents[0][2]
        number = sum([n for n, _, _ in documents])
        yield category_term, (number, count, N)

    def mapper_4(self, category_term, number_count_N):
        """ 
        This mapper groups the occurences per category for each term
        Returns a key value pair of: term, ((category, number), count, N)
        """

        number, count, N = number_count_N
        category, term = category_term
        yield term, ((category, number), count, N)

    def reducer_4(self, term, category_number_count_N):
        """ 
        This reducer sums the occurences of each term for all categories and adds it to the occurences per category for each term
        Returns a key value pair of: term, ([((category, number), count, N)], occurence_overall)
        """

        categories = list(category_number_count_N)
        occurence_overall = sum([number for (_, number), _, _ in categories])

        yield term, (categories, occurence_overall)

    def mapper_5(self, term, categories_occurences):
        """
        This mapper adds the number of all occurences of each term to the occurences per category for each term and groups them by category and term
        Returns a key value pair of: (category, term), (number, occurence_overall, count_category, N)
        """

        categories, occurence_overall = categories_occurences
        for (category, number), count_category, N in categories:
            yield (category, term), (number, occurence_overall, count_category, N)

    def reducer_5(self, category_term, number_count_occurence_N):
        """ 
        This reducer calculates the chi squared value for each term and category.
        Returns a key value pair of: (category, term), chi_squared
        """

        number_count_occurence_N = list(number_count_occurence_N)
        for number_count_occurence in number_count_occurence_N:
            number, occurence, count, N = number_count_occurence
            A = number
            B = occurence - A
            C = count - A
            D = N - count - B
            chi_squared = N * (A*D - B*C)**2 / ((A+B)*(A+C)*(B+D)*(C+D))
            yield category_term, chi_squared

    def mapper_6(self, category_term, chi_squared):
        """ 
        This mapper groups the chi squared values by category.
        Returns a key value pair of: category, (term, chi_squared)
        """

        category, term = category_term
        yield category, (term, chi_squared)

    def reducer_6(self, category, term_chi_squared):
        """ 
        This reducer sorts the chi squared values and returns the 75 highest values for each category
        Returns a key value pair of: category, [term=chi_squared]
        """
        chi_squared_terms = list(term_chi_squared)
        chi_squared_terms.sort(key=lambda x: x[1], reverse=True)

        yield category, ', '.join(f"{x}={y}" for x, y in chi_squared_terms[:75])


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
            ),
            MRStep(
                mapper   = self.mapper_5,
                reducer  = self.reducer_5
            ),
            MRStep(
                mapper   = self.mapper_6,
                reducer  = self.reducer_6
            )
        ]
   
if __name__ == '__main__':
    with open('./data/stopwords.txt', 'r') as f:
        stopwords = set(f.read().splitlines())


    myjob1 = ChiSquaredProcessor()
    with myjob1.make_runner() as runner:
        runner.run()
        
        for key, value in myjob1.parse_output(runner.cat_output()):           
            print(key, value, "\n", end='')

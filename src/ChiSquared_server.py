import json
import re
import os
from pathlib import Path
from mrjob.job import MRJob
from mrjob.job import MRStep





'''
count_term .. #documents in category with a word (e.g easy, electronics , 2)
number_of_occurences.. #documents where a word overall occured accross all categories - count term
category_count.. #categories
N.. total number of documents

A = count_term
B = number_of_occurences - count_term
C = category_count - count_term
D = N - category_count - B
'''


class MRChiSquare(MRJob):
    
    def configure_args(self):
        super(MRChiSquare, self).configure_args()

        self.FILES = [str(Path(__file__).parent.parent / 'Exercise_1' / 'stopwords.txt')]
        self.add_file_arg('--stopwords', default='stopwords.txt')
        self.add_passthru_arg('-k', type=int, default=75)


    def init_stopwords(self):
        with open(self.options.stopwords, 'r') as file:
            self.stopwords = set(file.read().splitlines())
    
    
    
    def mapper_preprocessing(self, _, value):
        # load reviews
        review = json.loads(value)
        review_text = review['reviewText']
        category = review['category']

        # tokenize words and case fold
        tokens = re.findall(r'\b[A-Za-z]+\b|[(){}\[\].!?,;:+=\-_"\'`~#@&*%€$§\\/]', review_text.lower())

        # remove stopwords and duplicates in each document
        document = [token for token in set(tokens) if token not in self.stopwords and len(token)>1]
        
        yield (category, None), 1
        
        for token in document:
            yield (category, token), 1
    
    
    # out: token and their occurence by category
    def reducer_1(self, category_token, count):
        category, token = category_token
        yield token, (category, sum(count))

    
    # out: token and their # of occurence within a category and total # of occurence across all categories
    def reducer_2(self, token, category_count):
        token_counts_by_category = {}
        
        if token is not None:
            yield None, token

        for category, count in category_count:
            if category not in token_counts_by_category:
                token_counts_by_category[category] = 0

            token_counts_by_category[category] = count # of docs where token occurs

        total_count_of_token = sum(token_counts_by_category.values())

        for category, count_by_category in token_counts_by_category.items():
            yield category, (token, count_by_category, total_count_of_token)

    
    # out: category and top 75 tokens with chi squared value
    def reducer_chi_squared(self, category, values):
        
        if category is None:
            yield None, sorted(values)
            return

        counts = {token: (count_by_category, total_count_of_token) for token, count_by_category, total_count_of_token in values}
        number_of_reviews_in_cat, N = counts.pop(None)
        result = []

        for token, (A, total_count_of_token) in counts.items():
            B = total_count_of_token - A
            C = number_of_reviews_in_cat - A
            D = N - A - B - C

            chi_squared = N * ((A * D - B * C) ** 2) / ((A + B) * (A + C) * (B + D) * (C + D))
            result.append((chi_squared, token))

        yield category, sorted(result, reverse=True)[:self.options.k]

        
        
    def steps(self):
        return [
            MRStep(
                mapper_init=self.init_stopwords,
                mapper=self.mapper_preprocessing,
                reducer=self.reducer_1
            ),
            MRStep(
                reducer=self.reducer_2
            ),
            MRStep(
                reducer=self.reducer_chi_squared
            )
        ]
    

if __name__ == '__main__':
    MRChiSquare.run()
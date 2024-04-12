import json
import re
from mrjob.job import MRJob
from mrjob.job import MRStep
from collections import defaultdict



'''
for local execution:
python src/ex1.py data/reviews_devset.json > output/output.txt

---Disclaimer--- 
Current logic computes the chi square values based on word counts in respective category, which is most likely not the ideal approach.
The document view of term occurence is currently not implemented; meaning, computing the chi square values based on the number of occurences of a term
in multiple reviews within a category. 

'''


# Load stopwords into defaultdict for efficient lookup
stopwords = defaultdict(int)
with open("data/stopwords.txt",'r') as f:
    stopwords = [line.strip() for line in f]


class MRChiSquare(MRJob):
    
    # input: value contains path to data
    # output: category, token
    # tokenizing the reviews
    def tokenizer(self, key, value):
        # load reviews
        review = json.loads(value)
        review_text = review['reviewText']
        category = review['category']

        # tokenize words
        tokens = re.findall(r'\b[A-Za-z]+\b|[(){}\[\].!?,;:+=\-_"\'`~#@&*%€$§\\/]', review_text)

        for token in tokens:
            yield category, token


    # input: category, term
    # output: category, term
    # case folding to lower case 
    def case_folder(self, category, term):
        yield category, term.lower()


    # input: category, term
    # output: (category, term), 1
    # load stopwords
    def stopword_remover(self, category, term):
        # load stopwords
        if (term not in stopwords and len(term) > 1):
            yield (category, term), 1
    

    # input: (category, term), 1
    # output: category, (character, sum_of_count)
    # calculates term frequencies by category
    def reducer_count_terms(self, category_term, count):
        category, term = category_term 
        yield category, (term, sum(count))


    # input: category, (term, sum_of_count)
    # output: term, (category, dict)
    # first step: calculate total number of terms by category
    # second step: calculate total count for a term by category
    # third step: calculate intermediate A, B, C, D values
    def reducer_chi_square_intermediate(self, category, term_counts):
        category_counts = defaultdict(int)
        term_counts_by_cat = {}
        
        for token, count in term_counts:
            category_counts['total_count'] += count # total word counts in/per category
            term_counts_by_cat[token] = count # occurence of word in/per category
        
        for term, term_count in term_counts_by_cat.items():
            category_counts['term_count'] = term_count # A
            category_counts['not_term_count'] = category_counts['total_count'] - term_count # B
            category_counts['term_category_count'] = term_count
            category_counts['not_term_category_count'] = category_counts['total_count'] - term_count
            yield term, (category, category_counts.copy())


    # input: term, (category, dict) 
    # output: category, (term, chi_square)
    # process intermediate results and compute chi square
    def reducer_chi_square_final(self, term, category_counts):
        term_total_count = 0
        term_category_counts = {}
        total_count_of_words = 0

        for category, counts in category_counts:
            term_total_count += counts['term_count'] # total count of term t across all categories
            term_category_counts[category] = counts['term_category_count'] # total count of words in a category
            total_count_of_words += counts['total_count'] # total count of words across all categories

        # probably wrong: needs to be fixed
        # current term total count * number of category - current term total count
        not_term_total_count = term_total_count * len(category) - term_total_count

        for category, term_category_count in term_category_counts.items():
            not_term_category_count = term_total_count - term_category_count
            chi_square = self.calculate_chi_square(term_total_count, not_term_total_count, term_category_count, not_term_category_count, total_count_of_words)
            yield category, (term, chi_square)
    

    # input: category, (term, chi_square)
    # output: category, (list of "term:chisquare")
    def reducer_output(self, category, term_chi_square):
        # Sort terms by chi-square value and yield top 75 terms per category
        sorted_terms = sorted(term_chi_square, key=lambda x: x[1], reverse=True)[:75]
        yield category, ' '.join(f"{x}:{y}" for x, y in sorted_terms)


    def steps(self):
        return [
            MRStep(mapper=self.tokenizer),
            MRStep(mapper=self.case_folder),
            MRStep(mapper=self.stopword_remover,
                   reducer=self.reducer_count_terms),
            MRStep(reducer=self.reducer_chi_square_intermediate),
            MRStep(reducer=self.reducer_chi_square_final),
            MRStep(reducer=self.reducer_output)

        ]
    

    def calculate_chi_square(self, term_count, not_term_count, term_category_count, not_term_category_count, total_count_of_words):
        A = term_category_count
        B = not_term_category_count
        C = term_count - term_category_count
        D = not_term_count - not_term_category_count
        N = total_count_of_words 
        numerator = N * (A * D - B * C) ** 2
        denominator = (A + B) * (A + C) * (B + D) * (C + D)

        return numerator / denominator


if __name__ == '__main__':
    MRChiSquare.run()
from mrjob.job import MRJob
from mrjob.step import MRStep
# from mr3px.csvprotocol import CsvProtocol
import re
import json

stopwords = set()

class ChiSquaredProcessor(MRJob):
    def mapper_category_contains_term(self, _, line):
        # Mapper for counting the number of documents in which a term appears in a category
        data = json.loads(line)

        category = data.get('category', '')
        reviewText = data.get('reviewText', '')
        word_list = re.split('[^a-zA-Z<>^|]+', reviewText.lower())
        # make wordlist to set, to only count each word once per review (occurence in document)
        word_set = set([word for word in word_list if word not in stopwords and word.strip() != '' and len(word) > 1])
        
        yield category, list(word_set)
        #for word in word_set:
        #    if word not in stopwords and word.strip() != '' and len(word) > 1:
        #        yield (category,  word), 1


    def reducer_count_categories_contains_term(self, category, compromised_reviewText):
        # Reducer for counting the number of documents in which a term appears in a category
        compromised_reviews = list(compromised_reviewText)
        yield category, (len(compromised_reviews), compromised_reviews)

    def mapper_term_for_categories(self, category, count_reviews):
        # Mapper for counting the all different combinations of terms occuring in a all categories
        count, reviews = count_reviews
        for review in reviews:
            for term in review:
                yield (category, term), (1, count)

    def reducer_term_for_categories(self, category, counts):
        # Reducer for collecting each category of t
        documents = list(counts)
        count = documents[0][1]
        number = sum([n for n, _ in documents])
        yield category, (number, count)

    def mapper_3(self, term, category_counts):
        # Mapper for counting the all different combinations of terms occuring in a all categories
        for cat, cnt in category_counts:
            yield cat, (term, count)

        #yield term, category_counts

    def reducer_3(self, term, category_counts):
        token_counts = list(category_counts)
        #A =
        #C =


        yield term, token_counts

    def steps(self):
        return [
            MRStep(
                mapper   = self.mapper_category_contains_term,
                reducer  = self.reducer_count_categories_contains_term
            ),
            MRStep(
                mapper   = self.mapper_term_for_categories,
                reducer  = self.reducer_term_for_categories
            )
            #MRStep(
            #    mapper   = self.mapper3,
            #    reducer  = self.reducer3
            #)
        ]
   
if __name__ == '__main__':
    with open('./stopwords.txt', 'r') as f:
        stopwords = set(f.read().splitlines())


    myjob1 = ChiSquaredProcessor()
    with myjob1.make_runner() as runner:
        runner.run()
        
        for key, value in myjob1.parse_output(runner.cat_output()):           
            print(key, value, "\n", end='')

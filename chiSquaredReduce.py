from mrjob.job import MRJob
from mrjob.step import MRStep
# from mr3px.csvprotocol import CsvProtocol
import re
import json

WORD_RE = re.compile('[a-zA-Z]+')

class ChiSquaredProcessor(MRJob):
    def mapper(self, _, line):
 
        data = json.loads(line)
        
        # Extract review text from JSON data
        review_text = data.get('reviewText', '')

        #print(review_text)
        # Tokenize review text
        word_list = re.split('[^a-zA-Z<>^|]+', review_text)
              
        #for loop through the terms in pre-processed list
        for word in word_list:
            yield (word, 1)
                
    def reducer_count(self, word, counts):
        # sums up the values of all appearances of the term
        yield  (word, sum(counts))
    
    def steps(self):
        return [
            MRStep(mapper  = self.mapper,
                   reducer = self.reducer_count)
        ]
   
if __name__ == '__main__':
    myjob1 = ChiSquaredProcessor()
    with myjob1.make_runner() as runner:
        runner.run()
        
        for key, value in myjob1.parse_output(runner.cat_output()):           
            print(key, value, "\n", end='')

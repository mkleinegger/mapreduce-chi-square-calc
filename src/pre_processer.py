from typing import Generator
from mrjob.job import MRJob
from mrjob.step import MRStep
from subtask import Subtask
import re

class PreProcessor(Subtask):

    def __init__(self, stopword_file: str = 'data/stopwords.txt', stopwords: set[str] = None):
        if stopwords is not None:
            self.stopwords = stopwords
        else:
            with open(stopword_file, 'r') as file:
                self.stopwords = set(file.readlines())


    def mapper_tokenization(self, key: tuple[str, str], data: tuple[str, str]):
        category, text = data

        # tokenises each line by using whitespaces, tabs, digits, and the characters ()[]{}.!?,;:+=-_"'`~#@&*%€$§\/ as delimiters 
        words = re.split('[^a-zA-Z<>^|]+', text)

        for word in words:
            yield key, (category, word)


    def mapper_case_fold(self, key: tuple[str, str], data: tuple[str, str]):
        category, token = data
        yield key, (category, token.lower())


    def mapper_stopword_removal(self, key: tuple[str, str], data: tuple[str, str]):
        category, token = data

        if len(token) > 1 and (token not in self.stopwords):
            yield key, (category, token)


    def steps(self) -> list[MRStep]:
        return [
            MRStep(mapper=self.mapper_tokenization),
            MRStep(mapper=self.mapper_case_fold),
            MRStep(mapper=self.mapper_stopword_removal),
        ]
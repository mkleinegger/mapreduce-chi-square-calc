import json
from typing import Generator
from mrjob.step import MRStep
from subtask import Subtask

class ParseJson(Subtask):

    def mapper(self, _, line: str):
        data = json.loads(line)
        yield (data['reviewerID'], data['asin']), (data['category'], data['reviewText'])

    def steps(self) -> list[MRStep]:
        return [
            MRStep(mapper=self.mapper)
        ]
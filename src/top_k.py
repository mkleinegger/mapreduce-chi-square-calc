from subtask import Subtask
from mrjob.step import MRStep

class TopK(Subtask):

    def __init__(self, k) -> None:
        self.k = k
    

    def combiner(self, key: str, data: list[any]):
        top_k = sorted(data, reverse=True)[:self.k]

        for value in top_k:
            yield key, value


    def reducer(self, key: str, data: list[any]):
        top_k = sorted(data, reverse=True)[:self.k]

        yield key, tuple(top_k)


    def steps(self) -> list[MRStep]:
        return [
            MRStep(combiner=self.combiner, reducer=self.reducer)
        ]
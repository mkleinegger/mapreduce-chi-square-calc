from mrjob.step import MRStep

class Subtask():

    def steps(self) -> list[MRStep]:
        raise NotImplementedError
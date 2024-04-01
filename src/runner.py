from mrjob.job import MRJob
from mrjob.step import MRStep
from chi_squared import ChiSquared
from pre_processer import PreProcessor
from parse_json import ParseJson
from top_k import TopK


class Runner(MRJob):

    _TASKS = [
        ParseJson(),
        PreProcessor(),
        ChiSquared(),
        TopK(75),
    ]
    _STEPS = [step for task in _TASKS for step in task.steps()]

    def steps(self) -> list[MRStep]:
        return self._STEPS


if __name__ == '__main__':
    job = Runner()

    with job.make_runner() as runner:
        runner.run()

        for key, values in job.parse_output(runner.cat_output()):
            values: list[list[float, str]]
            value_strings = [f'{value[1]}:{value[0]}' for value in values]
            print(' '.join([key] + value_strings))
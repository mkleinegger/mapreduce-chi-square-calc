from chiSquaredReduce import ChiSquaredProcessor
if __name__ == '__main__':
    job = ChiSquaredProcessor()

    with job.make_runner() as runner:
        runner.run()

        results = {}
        sorted_merged_terms = None
        for key, values in job.parse_output(runner.cat_output()):
            if key is None:
                sorted_merged_terms = ' '.join(values)
            else:
                results[key] = ' '.join(f"{x}={y}" for x, y in values)

        sorted_dict = dict(sorted(results.items()))
        for key, value in sorted_dict.items():
            if key is not None:
                print(key, value, "\n", end='')

        print(sorted_merged_terms)

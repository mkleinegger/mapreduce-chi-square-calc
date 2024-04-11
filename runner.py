from chiSquaredReduceOptimized import ChiSquaredProcessor

if __name__ == '__main__':
    job = ChiSquaredProcessor()

    with job.make_runner() as runner:
        runner.run()

        results = {}
        for key, values in job.parse_output(runner.cat_output()):
            if key is None:
                results[key] = ' '.join(values)
            else:
                results[key] = ' '.join(f"{x}={y}" for x, y in values)

        for key, value in results.items():
            if key is not None:
                print(key, value, "\n", end='')

        print(results[None])

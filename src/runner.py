from chi_squared import ChiSquaredJob


if __name__ == '__main__':
    job = ChiSquaredJob()

    with job.make_runner() as runner:
        runner.run()
        output = sorted(map(tuple, job.parse_output(runner.cat_output())))
        tokens = sorted({token for _, values in output for _, token in values})

        for key, values in output:
            values: list[list[float, str]]
            value_strings = [f'{value[1]}:{value[0]}' for value in values]
            print(' '.join([key] + value_strings))
                
        print(' '.join(tokens))
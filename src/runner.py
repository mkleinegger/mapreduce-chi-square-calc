from chi_squared import ChiSquaredJob


if __name__ == '__main__':
    job = ChiSquaredJob()

    with job.make_runner() as runner:
        runner.run()
        output = {category: values for category, values in job.parse_output(runner.cat_output())}
        tokens = output.pop(None)

        for key, values in sorted(output.items()):
            values: list[list[float, str]]
            value_strings = [f'{value[1]}:{value[0]}' for value in values]
            print(' '.join([key] + value_strings))
                
        print(' '.join(tokens))
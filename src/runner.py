from chi_squared import ChiSquared

"""
Runner script for the chi-squared feature selection algorithm.
Formats the output according to the assignment requirements and collects the
dictionary of tokens.
"""

if __name__ == '__main__':
    job = ChiSquared()

    with job.make_runner() as runner:
        runner.run()
        output = sorted(map(tuple, job.parse_output(runner.cat_output())))
        tokens = sorted({token for _, values in output for _, token in values})

        # sorted 
        for key, values in output:
            values: list[list[float, str]]
            value_strings = [f'{value[1]}:{value[0]}' for value in values]
            print(' '.join([f'<{key}>'] + value_strings))
                
        print(' '.join(tokens))
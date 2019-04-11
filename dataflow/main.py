from __future__ import absolute_import

import argparse
import logging
import re
import time

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(input_name, output_name, argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='gs://dotz-hiring-datalake/raw/{}.csv'.format(input_name),
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        default='gs://dotz-hiring-datalake/processed/{}'.format(output_name),
                        help='Output file to write results to.')

    (known_args, pipeline_args) = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--runner=DataflowRunner',
        '--project=dotz-hiring',
        '--staging_location=gs://dotz-hiring-datalake/staging',
        '--temp_location=gs://dotz-hiring-datalake/temp',
        '--job_name=job-test-{}'.format(str(time.time()).replace('.', '-')),
    ])

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | 'ReadTextFile({})'.format(input_name) >> ReadFromText(known_args.input)

        # Count the occurrences of each word.
        counts = (
            lines
            | 'Split' >> (beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x)).with_output_types(unicode))
            | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
            | 'GroupAndSum' >> beam.CombinePerKey(sum))

        # Format the counts into a PCollection of strings.
        def format_result(word_count):
            (word, count) = word_count
            return '{}: {}'.format(word, count)

        output = counts | 'Format' >> beam.Map(format_result)
        output | 'WriteTextFile({})'.format(output_name) >> WriteToText(known_args.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    input_name = 'comp_boss'
    output_name = 'components'

    run(input_name, output_name)
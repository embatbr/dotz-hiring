from __future__ import absolute_import

import json
import logging
import time

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


BASE_PIPELINE_ARGS = [
    '--runner=DataflowRunner',
    '--project=dotz-hiring',
    '--staging_location=gs://dotz-hiring-datalake/staging',
    '--temp_location=gs://dotz-hiring-datalake/temp'
]

CSV_DELIMITER = ','


def get_header(input_name):
    with open('storage/raw/{}.csv'.format(input_name)) as f:
        header = f.readline()
        return header[:-1]


def csv2json(fields):
    def _internal(line):
        splitted_line = line.split(CSV_DELIMITER)
        return { key:value for (key, value) in zip(fields, splitted_line) }

    return _internal


def run_pipeline(input_name, output_name):
    header = get_header(input_name)
    fields = header.split(CSV_DELIMITER)

    input_path = 'gs://dotz-hiring-datalake/raw/{}.csv'.format(input_name)
    output_path = 'gs://dotz-hiring-datalake/processed/{}'.format(output_name)

    pipeline_args = [
        '--job_name={}-{}'.format(output_name, str(time.time()).replace('.', '-')),
        '--input={}'.format(input_path),
        '--output={}'.format(output_path)
    ]

    pipeline_args.extend(BASE_PIPELINE_ARGS)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as pipeline:
        lines = pipeline | ReadFromText(input_path)

        # not so bright way to remove a CSV header
        lines = lines | 'RemoveHeader' >> beam.Filter(lambda line: line != header)
        objs = lines | 'CSV2JSON' >> beam.Map(csv2json(fields))
        # TODO rest of logic goes here
        dumped_objs = objs | 'DumpJSONs' >> beam.Map(json.dumps)

        dumped_objs | WriteToText(output_path)


def run():
    run_pipeline('comp_boss', 'components')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    run()

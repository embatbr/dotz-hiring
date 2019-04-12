from __future__ import absolute_import

import argparse
from datetime import datetime as dt
import json
import logging
import time

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import BigQueryDisposition
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

import sys
print sys.path


PROCESSES = {
    'components': {
        'source': 'comp_boss',
        'target': 'components'
    },
    'materials': {
        'source': 'bill_of_materials',
        'target': 'materials'
    },
    'pricing': {
        'source': 'price_quote',
        'target': 'pricing'
    }
}

BASE_PIPELINE_ARGS = [
    '--runner=DataflowRunner',
    '--project=dotz-hiring',
    '--staging_location=gs://dotz-hiring-datalake/staging',
    '--temp_location=gs://dotz-hiring-datalake/temp'
]

CSV_DELIMITER = ','


def get_header(source):
    with open('storage/raw/{}.csv'.format(source)) as f:
        header = f.readline()
        return header[:-1]


def get_schema(name):
    def type2func(_type):
        if _type == 'STRING':
            return lambda x: x
        if _type == 'INTEGER':
            return lambda x: int(x)
        if _type == 'FLOAT':
            return lambda x: float(x)
        if _type == 'BOOLEAN':
            return lambda x: x.lower() == 'yes'
        if _type == 'DATE':
            return lambda x: dt.strptime(x, '%Y-%m-%d').strftime('%Y-%m-%d')

    with open('schemas/{}.json'.format(name)) as f:
        bq_schema = json.load(f)

        schema = dict()
        for obj in bq_schema:
            schema[obj['name']] = {
                'func': type2func(obj['type']),
                'required': True if obj['mode'] == 'REQUIRED' else False
            }

        return (bq_schema, schema)


def csv2json(fields):
    def _internal(line):
        splitted_line = line.split(CSV_DELIMITER)
        return { key:value for (key, value) in zip(fields, splitted_line) }

    return _internal


def process(schema):
    def _convert(func, value):
        try:
            return func(value)
        except:
            return None

    def _internal(obj):
        proc_obj = dict()

        for (key, value) in obj.items():
            proc_value = None if value == 'NA' else value
            if proc_value is not None:
                func = schema[key]['func']
                proc_value = _convert(func, proc_value)

            if schema[key]['required'] and proc_value is None:
                return None

            proc_obj[key] = proc_value

        return proc_obj

    return _internal


def run_pipeline(source, target):
    header = get_header(source)
    fields = header.split(CSV_DELIMITER)

    (bq_schema, schema) = get_schema(target)

    input_path = 'gs://dotz-hiring-datalake/raw/{}.csv'.format(source)
    output_path = 'gs://dotz-hiring-datalake/processed/{}.json/part'.format(target)

    pipeline_args = [
        '--job_name={}-{}'.format(target, str(time.time()).replace('.', '-')),
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
        proc_objs = objs | 'ProcessJSONs' >> beam.Map(process(schema))
        filtered_proc_objs = proc_objs | 'FilterEmpties' >> beam.Filter(lambda x: x)

        dumped_objs = filtered_proc_objs | 'DumpJSONs' >> beam.Map(json.dumps)
        dumped_objs | WriteToText(output_path)

        filtered_proc_objs | WriteToBigQuery(
            'dotz-hiring:tubulation.{}'.format(target),
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=BigQueryDisposition.CREATE_NEVER
        )


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--process', dest='process', required=True)
    (args, _) = parser.parse_known_args()

    process = args.process
    source = PROCESSES[process]['source']
    target = PROCESSES[process]['target']

    run_pipeline(source, target)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    run()

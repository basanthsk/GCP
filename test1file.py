import pandas as pd
import json,os
import apache_beam as beam
# from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
# import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions

# with open('test1file.txt','r') as fs:
#     for line in fs:
#         row=json.loads(line)
#         if row['content']:
#             print(pd.read_json(row['content']))
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './credentils.json'
# Dflow_option = ['--project = iucc-assaf-anderson', '--job_name = bqetl4', '--temp_location = gs://dataflow-iucc-assaf-anderson/temp','--staging_location = gs://dataflow-iucc-assaf-anderson/staging']
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'iucc-assaf-anderson'
google_cloud_options.job_name = 'bqetl4'
google_cloud_options.staging_location = 'gs://dataflow-iucc-assaf-anderson/staging'
google_cloud_options.temp_location = 'gs://dataflow-iucc-assaf-anderson/temp'
options.view_as(StandardOptions).runner = 'Dataflow'

infile = 'gs://dataflow-iucc-assaf-anderson/test_file.txt'
# outfile='out1file.txt'
outfile = 'gs://dataflow-iucc-assaf-anderson/onefile_data'
logfile = 'gs://dataflow-iucc-assaf-anderson/logfile'

class JsonCoder(object):
    """A JSON coder interpreting each line as a JSON string."""

    def encode(self, x):
        return json.dumps(x)

    def decode(self, x):
        return json.loads(x)


def print_item(item):
    if item['content'] != 'notParse':
        print(item['content'])


class DimTrans(beam.DoFn):

    def process(self, element):
        import pandas as pd
        import json
        import apache_beam as beam
        try:
            data = pd.read_json(element['content'])
            title = data[:1]
            data = data[1:]
            for idx, item in data.iterrows():
                for dim, axis in item.items():
                    label = (title[dim].values[0])
                    d = {u'fbkey': '{}'.format(element['fbkeyl2']),
                         'axisDim': int(dim),
                         'axisOrder': int(idx),
                         'axisValue': float(axis),
                         'axisTitle': u'{}'.format(label)}
                    yield json.dumps(d)

        except Exception as e:
            beam.pvalue.TaggedOutput('exception', element['physical_measurement'])


if __name__ == '__main__':
    with beam.Pipeline(options=options) as pipeline:
        data,log = (pipeline
                     | beam.io.ReadFromText(infile, coder=JsonCoder())
                     | beam.Filter(lambda row: all([row['content'] != 'notParse', row['type'] == 'measurement']))
                     # | beam.Map(lambda e : (e['content'],e['physical_measurement']))
                     # | 'Print Results' >> beam.ParDo(DimTrans()).with_outputs('exception', main='data')
                     | 'trans1' >> beam.Map(DimTrans())
                     )

        data | beam.io.WriteToText(outfile)
        # log | 'exception' >> beam.io.WriteToText(logfile)
        pipeline.run()

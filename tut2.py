import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


options = PipelineOptions()
pipeline = beam.Pipeline('DirectPipelineRunner')
airports = (pipeline
   | beam.Read(beam.io.TextFileSource('airports.csv.gz'))
   | beam.Map(lambda line: next(csv.reader([line])))
   | beam.Map(lambda fields: (fields[0], (fields[21], fields[26])))
)

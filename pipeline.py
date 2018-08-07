import argparse
import logging
import datetime, os
import apache_beam as beam
import helpers

PROJECT_ID = 'udemy-data-engineer-210920'
BUCKET_ID = 'udemy-data-engineer-210920'
BUCKET_FOLDER = 'dataflow-pipeline-py'


def run():

  parser = argparse.ArgumentParser(description='BigQuery as source & side input')
  parser.add_argument('--bucket',
                      default=BUCKET_ID,
                      help='Specify Cloud Storage bucket for output')
  parser.add_argument('--folder',
                      default=BUCKET_FOLDER,
                      help='Specify Cloud Storage bucket folder for output')
  parser.add_argument('--project',
                      default=PROJECT_ID,
                      help='Specify Google Cloud project')

  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument('--local',
                     action='store_true')
  group.add_argument('--dataflow',
                     action='store_true')

  opts = parser.parse_args()

  if opts.local:
    runner='DirectRunner'
  elif opts.dataflow:
    runner='DataFlowRunner'

  assert runner, 'Unknown runner'

  bucket = opts.bucket
  project = opts.project

  #    Limit records if running local, or full data if running on the cloud
  limit_records=''
  if runner == 'DirectRunner':
     limit_records='LIMIT 3000'

  get_java_query='SELECT content FROM [fh-bigquery:github_extracts.contents_java_2016] {0}'.format(limit_records)

  argv = [
    '--project={0}'.format(project),
    '--job_name=cooljob',
    '--save_main_session',
    '--staging_location=gs://{0}/staging/'.format(bucket),
    '--temp_location=gs://{0}/staging/'.format(bucket),
    '--runner={0}'.format(runner)
    ]

  p = beam.Pipeline(argv=argv)


  # Read the table rows into a PCollection (a Python Dictionary)
  bigqcollection = p | 'ReadFromBQ' >> beam.io.Read(beam.io.BigQuerySource(project=project,query=get_java_query))

  popular_packages = is_popular(bigqcollection) # main input

  help_packages = needs_help(bigqcollection) # side input

  # Use side inputs to view the help_packages as a dictionary
  results = popular_packages | 'Scores' >> beam.FlatMap(lambda element, the_dict: compositeScore(element,the_dict), beam.pvalue.AsDict(help_packages))

  # Write out the composite scores and packages to an unsharded csv file
  output_results = 'gs://{0}/javahelp/Results'.format(bucket)
  results | 'WriteToStorage' >> beam.io.WriteToText(output_results,file_name_suffix='.csv',shard_name_template='')

  # Run the pipeline (all operations are deferred until run() is called).


  if runner == 'DataFlowRunner':
     p.run()
  else:
        p.run().wait_until_finish()
  logging.getLogger().setLevel(logging.INFO)


if __name__ == '__main__':
      run()

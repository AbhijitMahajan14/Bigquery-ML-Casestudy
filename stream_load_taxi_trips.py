import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery

class TaxiDataTransform(beam.DoFn):
    def process(self, element):
        import csv
        from datetime import datetime
        reader = csv.DictReader([element])
        for row in reader:
            yield {
                'trip_id': row['trip_id'],
                'taxi_id': row['taxi_id'],
                'trip_start_timestamp': datetime.strptime(row['trip_start_timestamp'], '%Y-%m-%dT%H:%M:%S.%f'),
                'trip_end_timestamp': datetime.strptime(row['trip_end_timestamp'], '%Y-%m-%dT%H:%M:%S.%f'),
                'trip_seconds': int(row['trip_seconds']),
                'trip_miles': float(row['trip_miles']),
                'fare': float(row['fare']),
                'tips': float(row['tips']),
                'tolls': float(row['tolls']),
                'trip_total': float(row['trip_total']),
                'payment_type': row['payment_type'],
                'company': row['company'],
                'pickup_centroid_latitude': float(row['pickup_centroid_latitude']),
                'pickup_centroid_longitude': float(row['pickup_centroid_longitude']),
                'dropoff_centroid_latitude': float(row['dropoff_centroid_latitude']),
                'dropoff_centroid_longitude': float(row['dropoff_centroid_longitude'])
            }


def run():
    options = PipelineOptions(
        project='skilful-gantry-426403-k1',
        region='us',
        runner='DataflowRunner',
        streaming=True,
        temp_location='gs://taxi_trips_chicago/temp',
    )
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic='projects/skilful-gantry-426403-k1/topics/nyc-taxi-topic')
            | 'TransformData' >> beam.ParDo(TaxiDataTransform())
            | 'WriteToBigQuery' >> WriteToBigQuery(
                'trips.chicago_taxi_case_study',
                schema='trip_id:STRING,taxi_id:STRING,trip_start_timestamp:TIMESTAMP,trip_end_timestamp:TIMESTAMP,trip_seconds:INTEGER,trip_miles:FLOAT,fare:FLOAT,tips:FLOAT,tolls:FLOAT,trip_total:FLOAT,payment_type:STRING,company:STRING,pickup_centroid_latitude:FLOAT,pickup_centroid_longitude:FLOAT,dropoff_centroid_latitude:FLOAT,dropoff_centroid_longitude:FLOAT',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    run()

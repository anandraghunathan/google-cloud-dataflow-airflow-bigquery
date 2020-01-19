import airflow
from airflow import models
from airflow.operators import bash_operator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

args = {
    'owner': 'Anand Raghunathan',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = models.DAG(
    dag_id='direct_load_gcs_to_bigquery', default_args=args, schedule_interval=None)

#create_dataset = bash_operator.BashOperator(
#    task_id='create_dataset',
#    bash_command='bq mk gcs_to_bq_dataset',
#    dag=dag)

gcs_sensor = GoogleCloudStorageObjectSensor(
    task_id='sensor_for-gcs',
    bucket=models.Variable.get('input_bucket_name'),
    #object='file-name-prefix' + date + '.gz',
    object='usa_names.csv',
    #google_cloud_conn_id='google-cloud-storage-default',
    dag=dag)

# [START gcs_to_bq]
load_csv = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_csv_on_gcs_to_bq',
    bucket = models.Variable.get('input_bucket_name'),
    source_objects = ['usa_names.csv'],
    destination_project_dataset_table='dataset.usa_names',
    #schema_objects=models.Variable.get('schema_fullpath')
    schema_fields=[
        {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'gender', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'year', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'number', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'created_date', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag)
# [END gcs_to_bq]

load_csv.set_upstream(gcs_sensor)

#delete_dataset = bash_operator.BashOperator(
#    task_id='delete_dataset',
#    bash_command='bq rm -rf gcs_to_bq_dataset',
#    dag=dag)

#create_test_dataset >> load_csv #>> delete_test_dataset
load_csv

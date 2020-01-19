"""
A simple Airflow DAG that is triggered externally by a Cloud Function (Yet to be done
when a file lands in a GCS bucket.

Once triggered the DAG performs the following steps:
1. Triggers a Google Cloud Dataflow job with the input file information received
   from the Cloud Function trigger.
2. Upon completion of the Dataflow job, the input file is moved to a
   gs://<target-bucket>/<success|failure>/YYYY-MM-DD/ location based on the
   status of the previous step.
"""

import datetime
import logging
import os

from airflow import configuration
from airflow import models
from airflow.contrib.hooks import gcs_hook
from airflow.contrib.operators import dataflow_operator
from airflow.operators import python_operator
from airflow.utils.trigger_rule import TriggerRule

# We set the start_date of the DAG to the previous date. This will
# make the DAG immediately available for scheduling.
YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

# We define some variables that we will use in the DAG tasks.
SUCCESS_TAG = 'success'
FAILURE_TAG = 'failure'

# An Airflow variable called gcs_completion_bucket is required.
# This variable will contain the name of the bucket to move the processed
# file to.
COMPLETION_BUCKET = models.Variable.get('completion_bucket_name')
DS_TAG = '{{ ds }}'
DATAFLOW_FILE = os.path.join(
    configuration.get('core', 'dags_folder'), 'dataflow', 'process_delimited.py')

# The following additional Airflow variables should be set:
# gcp_project:         Google Cloud Platform project id.
# gcp_temp_location:   Google Cloud Storage location to use for Dataflow temp location.
# email:               Email address to send failure notifications.
DEFAULT_DAG_ARGS = {
    'owner': 'Anand Raghunathan',
    'start_date': YESTERDAY,
    'email': models.Variable.get('email'),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'project_id': models.Variable.get('gcp_project'),
    'dataflow_default_options': {
        'project': models.Variable.get('gcp_project'),
        'temp_location': models.Variable.get('staging_bucket_fullpath'),
        'runner': 'DataflowRunner'
    }
}

def move_to_completion_bucket(target_bucket, target_infix, **kwargs):
    """A utility method to move an object to a target location in GCS."""
    # Here we establish a connection hook to GoogleCloudStorage.
    # Google Cloud Composer automatically provides a google_cloud_storage_default
    # connection id that is used by this hook.
    conn = gcs_hook.GoogleCloudStorageHook()

    # The external trigger (Google Cloud Function/Airflow GCS Sensor) that initiates
    # this DAG provides a dag_run.conf dictionary with event attributes that specify
    # the information about the GCS object that triggered this DAG.
    # We extract the bucket and object name from this dictionary.

    # source_bucket = kwargs['dag_run'].conf['bucket']
    # source_object = kwargs['dag_run'].conf['name']
    # completion_ds = kwargs['ds']

    source_bucket = models.Variable.get('input_bucket_name')
    source_object = models.Variable.get('input_filename')
    completion_ds = '{{ ds }}'

    target_object = os.path.join(target_infix, completion_ds, source_object)

    logging.info('Copying %s to %s',
                 os.path.join(source_bucket, source_object),
                 os.path.join(target_bucket, target_object))
    conn.copy(source_bucket, source_object, target_bucket, target_object)

    logging.info('Deleting %s',
                 os.path.join(source_bucket, source_object))
    conn.delete(source_bucket, source_object)

# Setting schedule_interval to None as this DAG is externally trigger by a Cloud Function.
# The following Airflow variables should be set for this DAG to function:
# bq_output_table: BigQuery table that should be used as the target for
#                  Dataflow in <dataset>.<tablename> format.
#                  e.g. lake.usa_names
# input_field_names: Comma separated field names for the delimited input file.
#                  e.g. state,gender,year,name,number,created_date
with models.DAG(dag_id='DataFlowToBigQuery',
                description='An Aiflow DAG triggered based on Airflow GCS sensor',
                schedule_interval=None, default_args=DEFAULT_DAG_ARGS) as dag:
    # Args required for the Dataflow job.
    job_args = {
        #'input': 'gs://{{ dag_run.conf["bucket"] }}/{{ dag_run.conf["name"] }}',
        #'output': models.Variable.get('bq_output_table'),
        #'fields': models.Variable.get('input_field_names'),

        'input': models.Variable.get('input_file_fullpath'),
        'output': models.Variable.get('bq_output_table'),
        'fields': models.Variable.get('input_field_names'),
        'load_dt': DS_TAG
    }

    # Main Dataflow task that will process and load the input delimited file.
    dataflow_task = dataflow_operator.DataFlowPythonOperator(
        task_id="process-csv-input-and-store-into-bigquery",
        py_file=DATAFLOW_FILE,
        options=job_args)

    # Here we create two conditional tasks, one of which will be executed
    # based on whether the dataflow_task was a success or a failure.
    success_move_task = python_operator.PythonOperator(task_id='success-move-to-completion',
                                                       python_callable=move_to_completion_bucket,
                                                       # A success_tag is used to move
                                                       # the input file to a success
                                                       # prefixed folder.
                                                       op_args=[COMPLETION_BUCKET, SUCCESS_TAG],
                                                       provide_context=True,
                                                       trigger_rule=TriggerRule.ALL_SUCCESS)

    failure_move_task = python_operator.PythonOperator(task_id='failure-move-to-completion',
                                                       python_callable=move_to_completion_bucket,
                                                       # A failure_tag is used to move
                                                       # the input file to a failure
                                                       # prefixed folder.
                                                       op_args=[COMPLETION_BUCKET, FAILURE_TAG],
                                                       provide_context=True,
                                                       trigger_rule=TriggerRule.ALL_FAILED)

    # The success_move_task and failure_move_task are both downstream from the
    # dataflow_task.
    dataflow_task >> success_move_task
    dataflow_task >> failure_move_task

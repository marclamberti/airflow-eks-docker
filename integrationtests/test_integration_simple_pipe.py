import pytest
import requests
import json
import os

class TestIntegrationSimplePipe:

    AIRFLOW_URL = "http://localhost:8080"

    def pause_dag(self, dag_id, pause):
        requests.get("{}/api/experimental/dags/{}/paused/{}".format(self.AIRFLOW_URL, dag_id, pause))
        status = json.loads(requests.get("{}/api/experimental/dags/{}/paused".format(self.AIRFLOW_URL, dag_id)).content)["is_paused"]
        return status

    def test_simple_pipe(self):
        """ Simple Pipe should run successfully """
        execution_date = "2020-05-21T12:00:00+00:00"
        dag_id = "simple_pipe"
        headers = {'Cache-Control': 'no-cache', 'Content-Type': 'application/json'}
        data = '{"execution_date": "%s"' % (execution_date)
        
        # Unpause DAG - Required to trigger it even manually
        assert self.pause_dag(dag_id, "false") == False, "The DAG {} is still in pause".format(dag_id)

        # Trigger the DAG
        if requests.post("{}/api/experimental/dags/{}/dag_runs".format(self.AIRFLOW_URL, dag_id), headers=headers, data=data).status_code != 200:
            raise Exception("Can't reach the API of Airflow")
        
        # Check if it is running as expected
        is_running = True
        while is_running:
            is_running = json.loads(requests.get("{}/api/experimental/dags/{}/dag_runs/{}".format(self.AIRFLOW_URL, dag_id, execution_date)).content)["status"]
        assert is_running == False, "The DAG {} didn't run as expected".format(dag_id)

        # pause DAG - Required to trigger it even manually
        assert self.pause_dag(dag_id, "true") == True, "The DAG {} did get paused as expected".format(dag_id)

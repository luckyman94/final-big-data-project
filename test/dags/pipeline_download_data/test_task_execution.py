import pytest
import uuid
from airflow.models import DagBag, TaskInstance, DagRun
from airflow.utils.state import State
from datetime import datetime


@pytest.fixture(scope="module")
def dagbag():
    return DagBag()


@pytest.fixture
def create_dag_run(dagbag):
    def _create_dag_run(dag_id, execution_date):
        dag = dagbag.get_dag(dag_id)
        run_id = f"test_run_{uuid.uuid4()}"  # Generate a unique run_id for each test
        dag_run = DagRun(
            dag_id=dag_id,
            execution_date=execution_date,
            start_date=datetime.now(),
            state=State.RUNNING,
            run_id=run_id
        )
        dag_run.dag = dag
        dag_run = dag.create_dagrun(
            run_id=run_id,
            execution_date=execution_date,
            state=State.RUNNING,
            conf={},
            external_trigger=False,
        )
        return dag_run

    return _create_dag_run


def test_scrap_netflix_execution(dagbag, mocker, create_dag_run):
    dag_id = 's3_dag'
    task_id = 'scrap_netflix'
    execution_date = datetime.now()

    # Create a DagRun
    create_dag_run(dag_id, execution_date)

    dag = dagbag.get_dag(dag_id)
    task = dag.get_task(task_id)

    ti = TaskInstance(task=task, execution_date=execution_date)

    # Mock the function to avoid actual execution
    mocker.patch('src.scraping.netflix_downloader.download_netflix_data')
    ti.run(ignore_ti_state=True)


def test_scrap_allocine_execution(dagbag, mocker, create_dag_run):
    dag_id = 's3_dag'
    task_id = 'scrap_allocine'
    execution_date = datetime.now()

    # Create a DagRun
    create_dag_run(dag_id, execution_date)

    dag = dagbag.get_dag(dag_id)
    task = dag.get_task(task_id)

    ti = TaskInstance(task=task, execution_date=execution_date)

    # Mock the function to avoid actual execution
    mocker.patch('src.scraping.allocine_scraper.run_scrap_allocine')
    ti.run(ignore_ti_state=True)




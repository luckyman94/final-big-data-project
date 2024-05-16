import pytest
from airflow.models import DagBag

@pytest.fixture(scope="module")
def dagbag():
    return DagBag()


def test_dag_loaded(dagbag):
    dag_id = 's3_dag'
    assert dag_id in dagbag.dags
    dag = dagbag.get_dag(dag_id)
    assert dag is not None
    assert len(dag.tasks) == 3


def test_task_dependencies(dagbag):
    dag_id = 's3_dag'
    dag = dagbag.get_dag(dag_id)

    scrap_netflix_task = dag.get_task('scrap_netflix')
    scrap_allocine_task = dag.get_task('scrap_allocine')
    upload_to_s3_task = dag.get_task('upload_to_s3')

    assert 'scrap_netflix' in [t.task_id for t in scrap_allocine_task.upstream_list]
    assert 'scrap_allocine' in [t.task_id for t in upload_to_s3_task.upstream_list]
    assert 'scrap_netflix' not in [t.task_id for t in upload_to_s3_task.upstream_list]


def test_dag_structure(dagbag):
    dag_id = 's3_dag'
    dag = dagbag.get_dag(dag_id)

    expected_task_order = ['scrap_netflix', 'scrap_allocine', 'upload_to_s3']
    tasks = dag.tasks
    task_order = [task.task_id for task in tasks]

    for task_id in expected_task_order:
        assert task_id in task_order

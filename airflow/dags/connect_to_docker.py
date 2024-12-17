from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
# from pendulum import datetime
from datetime import datetime
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.configuration import conf
from kubernetes.client import models as k8s


namespace = conf.get('kubernetes', 'NAMESPACE')

# This will detect the default namespace locally and read the
# environment namespace when deployed to Astronomer.
if namespace =='default':
    config_file = '/usr/local/airflow/.kube/config'
    in_cluster = False
else:
    in_cluster = True
    config_file = None

computational_resource = k8s.V1ResourceRequirements(
    limits={'cpu':'800m', 'memory':'500Mi'},
    requests={'cpu':'800m', 'memory':'500Mi'},
)


@dag(start_date=datetime.now(), schedule=None, catchup=False)
def exmaple_airflow():

    extract_mongo_task = DockerOperator(
        task_id='extract_mongo',
        image='mongo-extract:1.0',
        command=["python", "extract_mongo_clickhouse.py"],
        container_name='extract-mongo',
        docker_url='unix://var/run/docker.sock',
        network_mode='data-eng-network',
        mount_tmp_dir=False,
        auto_remove='success',
        environment={
            'CLICKHOUSE_HOST': 'localhost',
            'CLICKHOUSE_USER': 'datpd1',
            'CLICKHOUSE_PASSWORD': 'datpd1',
            'MONGO_HOST': 'localhost',
            'MONGO_USER': 'user',
            'MONGO_PASSWORD': 'password'
        }
        
    )
    kube_deploy = KubernetesPodOperator(
        namespace=namespace,
        image="hello-world",
        labels={"foo": "bar"},
        name="airflow-test-pod",
        task_id="task-one",
        in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
        cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
        config_file=config_file,
        container_resources=computational_resource,
        is_delete_operator_pod=True,
        get_logs=True,
    )
    extract_mongo_task >> kube_deploy
exmaple_airflow()
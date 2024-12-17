from airflow.decorators import dag
from airflow.operators.bash import BashOperator
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
def airflow_docker_operator():

    extract_mongo_task = DockerOperator(
        task_id='extract_mongo',
        image='mongo-extract:1.0',
        command=["python", "extract_mongo_clickhouse.py"],
        container_name='extract-mongo',
        docker_url='unix://var/run/docker.sock',
        network_mode='host',
        mount_tmp_dir=False,
        auto_remove='success',
        environment={
            'CLICKHOUSE_HOST': 'localhost',
            'CLICKHOUSE_USER': 'datpd1',
            'CLICKHOUSE_PASSWORD': 'datpd1',
            'DATABASE': 'houses',
            'TABLE': 'data_house_v2',
            'MONGO_DB': 'admin',
            'MONGO_COLLECTIONS': 'data_houses',
            'MONGO_HOST': 'localhost',
            'MONGO_USER': 'user',
            'MONGO_PASSWORD': 'password',
            
        }
    )

    fact_house_task = DockerOperator(
        task_id='fact_house',
        image='dbt-clickhouse-custom:1.0',
        command=["run", "--models", "fact_house"],
        container_name='fact-house',
        docker_url='unix://var/run/docker.sock',
        network_mode='host',
        mount_tmp_dir=False,
        auto_remove='success',
        environment={
            'HOST_CLICKHOUSE': 'localhost',
            'DBT_ENV_CUSTOM_ENV_SCHEMA': 'houses',
            'DBT_ENV_CUSTOM_ENV_CLICKHOUSE_USR': 'datpd1',
            'DBT_ENV_SECRET_CLICKHOUSE_PW': 'datpd1',
            'DBT_ENV_CUSTOM_ENV_CLICKHOUSE_TABLE': 'data_house_v2'


        }   
    )

    mart_region_house_sell_task = DockerOperator(
        task_id='mart_region_house_sell',
        image='dbt-clickhouse-custom:1.0',
        command=["run", "--models", "mart_region_house_sell"],
        container_name='mart-region-house-sell',
        docker_url='unix://var/run/docker.sock',
        network_mode='host',
        mount_tmp_dir=False,
        auto_remove='success',
        environment={
            'HOST_CLICKHOUSE': 'localhost',
            'DBT_ENV_CUSTOM_ENV_SCHEMA':'houses',
            'DBT_ENV_CUSTOM_ENV_CLICKHOUSE_USR': 'datpd1',
            'DBT_ENV_SECRET_CLICKHOUSE_PW': 'datpd1',
            'DBT_ENV_CUSTOM_ENV_CLICKHOUSE_TABLE': 'data_house_v2'

        }   
    )

    mart_spec_house_price_task = DockerOperator(
        task_id='mart_spec_house_price',
        image='dbt-clickhouse-custom:1.0',
        command=["run", "--models", "mart_spec_house_price"],
        container_name='mart_spec_house_price',
        docker_url='unix://var/run/docker.sock',
        network_mode='host',
        mount_tmp_dir=False,
        auto_remove='success',
        environment={
            'HOST_CLICKHOUSE': 'localhost',
            'DBT_ENV_CUSTOM_ENV_SCHEMA':'houses',
            'DBT_ENV_CUSTOM_ENV_CLICKHOUSE_USR': 'datpd1',
            'DBT_ENV_SECRET_CLICKHOUSE_PW': 'datpd1',
            'DBT_ENV_CUSTOM_ENV_CLICKHOUSE_TABLE': 'data_house_v2'


        } 

    
    )
    

    extract_mongo_task >> fact_house_task >> mart_region_house_sell_task
    fact_house_task >> mart_spec_house_price_task


airflow_docker_operator()
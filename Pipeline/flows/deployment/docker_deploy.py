from prefect.infrastructure.container import DockerContainer
from prefect.deployments import Deployment
from parameterized_flow import etl_parent_flow

docker_block = DockerContainer.load("de-project") 

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow, 
    name='docker-flow', 
    infrastructure=docker_block,
    files=["./config_02.py"]  # Add the path to your config.py file
)



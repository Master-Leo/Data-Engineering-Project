from prefect import Flow, Task
from prefect.infrastructure.container import DockerContainer
from prefect.deployments import Deployment
from econ_to_gcp import etl_econ_parent_flow
from demo_to_gcp import etl_demo_parent_flow
from geo_to_gcp import etl_geo_parent_flow

docker_block = DockerContainer.load("de-project") 

econ_dep = Deployment.build_from_flow(
    flow=etl_econ_parent_flow, 
    name='econ_deploy', 
    infrastructure=docker_block
    )

demo_dep = Deployment.build_from_flow(
    flow=etl_demo_parent_flow, 
    name='demo_deploy', 
    infrastructure=docker_block
    )

geo_dep = Deployment.build_from_flow(
    flow=etl_geo_parent_flow, 
    name='geo_deploy', 
    infrastructure=docker_block
    )

if __name__ == '__main__':
    econ_dep.apply()
    demo_dep.apply()
    geo_dep.apply()

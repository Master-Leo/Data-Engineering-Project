# build docker image

1. docker image build -t {docker_id}/{image_name}:{tag} . 
    - docker image build -t bruceleo31/project:census .

2. docker push {docker_id}/{image_name}:{tag}
    - docker push bruceleo31/project:census 

# Prefect deployment

1. python flows/deploy/deploy.py

2. prefect agent start --pool default-agent-pool

3. prefect config set PREFECT_API_URL='https://127.0.0.1:4200/api'

4. prefect deployment run flow/docker


Yes, you can have multiple Dockerfiles in a project. Each Dockerfile represents a separate Docker image and defines the instructions to build that image. Having multiple Dockerfiles can be useful when you have different components or services that require different dependencies or configurations.

You can name your Dockerfiles differently to reflect their purpose or the component they are associated with. For example, you can have `Dockerfile.app` for building an application image and `Dockerfile.worker` for building a worker image.

To build an image using a specific Dockerfile, you can use the `-f` flag with the `docker build` command. Here's an example:

```shell
docker build -t myapp -f Dockerfile.app .
```

This command will use the `Dockerfile.app` in the current directory to build the image with the tag `myapp`.

Similarly, you can specify the Dockerfile when running a container based on a specific image:

```shell
docker run myapp -f Dockerfile.app
```

This will run a container based on the `myapp` image, using the instructions specified in `Dockerfile.app`.

Having multiple Dockerfiles allows you to customize the build process for different components or services in your project.


# Kaggle API Directions
1. pip install kaggle
2. mv kaggle.json ~/.kaggle/kaggle.json
3. chmod 600 ~/.kaggle.kaggle.json


# Census API Directions 



# Welcome to your prefect.yaml file! You can you this file for storing and managing
# configuration for deploying your flows. We recommend committing this file to source
# control along with your flow code.

# Generic metadata about this project
name: deploy
prefect-version: 2.10.20

# build section allows you to manage and build docker images
build:
- prefect_docker.deployments.steps.build_docker_image:
    id: build_image
    requires: prefect-docker>=0.3.1
    image_name: project
    tag: census
    dockerfile: auto

# push section allows you to manage if and how this project is uploaded to remote locations
push:
- prefect_docker.deployments.steps.push_docker_image:
    requires: prefect-docker>=0.3.1
    image_name: '{{ build_image.image_name }}'
    tag: '{{ build_image.tag }}'
- prefect_gcp.deployments.steps.push_to_gcs:
    id: push_code
    requires: prefect-gcp>=0.4.3
    bucket: de_final_project_bucket
    folder: deploy

# pull section allows you to provide instructions for cloning this project in remote locations
pull:
- prefect_gcp.deployments.steps.pull_from_gcs:
    id: pull_code
    requires: prefect-gcp>=0.4.3
    bucket: '{{ push_code.bucket }}'
    folder: '{{ push_code.folder }}'

# the deployments section allows you to provide configuration for deploying flows
deployments:
- name: econ_deployment
  # tags: []
  # description: 
  schedule: 0 0 1 1 * * 
  # flow_name: null
  entrypoint: econ_to_gcp.py:etl_econ_parent_flow
  # parameters: {}
  work_pool:
    name: production-work-pool 
    # work_queue_name: null
    # job_variables: {}

- name: demo_deployment
  # description: 
  schedule: 0 0 1 1 * * 
  # flow_name: null
  entrypoint: demo_to_gcp.py:etl_demo_parent_flow
  # parameters: {}
  work_pool:
    name: production-work-pool 
    # work_queue_name: null
    # job_variables: {}

- name: geo_deployment
  # tags: []
  # description: 
  schedule: 0 0 1 1 * * 
  # flow_name: null
  entrypoint: geo_to_gcp.py:etl_geo_parent_flow
  # parameters: {}
  work_pool:
    name: production-work-pool 
    # work_queue_name: null
    # job_variables: {}
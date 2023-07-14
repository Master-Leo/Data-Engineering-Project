# build docker image

1. docker image build -t {docker_id}/{image_name}:{tag} . 
    - docker image build -t bruceleo31/project:census .

2. docker push {docker_id}/{image_name}:{tag}
    - docker push bruceleo31/project:census 

# Prefect deployment

1. python flows/deployment/docker_deploy.py

2. prefect agent start --pool default-agent-pool

3. prefect config set PREFECT_API_URL='https://127.0.0.1:4200/api'

4. prefect deployment run etl-parent-flow/docker-flow


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
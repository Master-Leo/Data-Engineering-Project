# Base Image
FROM python:3.9
WORKDIR /opt/prefect/flows
COPY requirements.txt .
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir

# Spark Stage
FROM gcr.io/datamechanics/spark:platform-3.1-dm14 as spark_stage
USER root
RUN useradd sparkuser
RUN groupadd --g 1024 groupcontainer
RUN usermod -a -G groupcontainer sparkuser
RUN chown -R :1024 ~/
RUN chmod 777 -R ~/
USER sparkuser
ENV PATH="/home/sparkuser/.local/bin:$PATH"
COPY --from=0 /opt/prefect/flows /opt/prefect/flows
# At the end of this stage, we have a Spark environment setup

# Prefect Stage
FROM prefecthq/prefect:2-python3.9
ADD flows /opt/prefect/flows
ADD flows/data /opt/prefect/flows

# Copy the necessary files or directories from the spark_stage
COPY --from=spark_stage /opt/prefect/flows /opt/prefect/flows
# If there are other files or directories that are needed, add additional COPY --from=spark_stage lines here.

# entrypoint
ENTRYPOINT ["/bin/sh"]

# # COMMAND to initialize prefect agent pool --docker
# CMD ["prefect", "worker", "start", "--pool", "production-pool"]


FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .
# COPY create_gcp_buckets.py .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows 
COPY flows/deployment/config.py /opt/prefect/flows/deployment/config.py

RUN mkdir -p /opt/prefect/data/economic

# # Set the command to run the create_gcp_buckets.py script
# CMD python create_gcp_buckets.py
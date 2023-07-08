FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows 
COPY flows/deployment/config_02.py /opt/prefect/flows/deployment/config_02.py

RUN mkdir -p /opt/prefect/data/economic

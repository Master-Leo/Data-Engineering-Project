FROM prefecthq/prefect:2.7.7-python3.9

COPY requirements.txt .

RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows/deploy/econ_to_gcp.py /opt/prefect_census_pipelines/flows/deploy/econ_to_gcp.py
COPY flows/deploy/demo_to_gcp.py /opt/prefect_census_pipelines/flows/deploy/demo_to_gcp.py
COPY flows/deploy/geo_to_gcp.py /opt/prefect_census_pipelines/flows/deploy/geo_to_gcp.py

RUN mkdir -p /opt/prefect/flows/data/demographic/state
RUN mkdir -p /opt/prefect/flows/data/demographic/city
RUN mkdir -p /opt/prefect/flows/data/demographic/zip_code

RUN mkdir -p /opt/prefect/flows/data/economic/state
RUN mkdir -p /opt/prefect/flows/data/economic/city
RUN mkdir -p /opt/prefect/flows/data/economic/zip_code

RUN mkdir -p /opt/prefect/flows/data/geographic/state
RUN mkdir -p /opt/prefect/flows/data/geographic/city
RUN mkdir -p /opt/prefect/flows/data/geographic/zip_code

COPY flows/deploy/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
WORKDIR /app

ENTRYPOINT ["/app/entrypoint.sh"]



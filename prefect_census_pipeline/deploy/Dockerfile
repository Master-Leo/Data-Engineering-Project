FROM prefecthq/prefect:2-python3.9
# RUN pip install --upgrade --trusted-host pypi.python.org --no-cache-dir
COPY requirements.txt .
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir
ADD flows /opt/prefect/flows

RUN mkdir -p /opt/prefect/flows/data/demographic/state
RUN mkdir -p /opt/prefect/flows/data/demographic/city
RUN mkdir -p /opt/prefect/flows/data/demographic/zip_code

RUN mkdir -p /opt/prefect/flows/data/economic/state
RUN mkdir -p /opt/prefect/flows/data/economic/city
RUN mkdir -p /opt/prefect/flows/data/economic/zip_code

RUN mkdir -p /opt/prefect/flows/data/geographic/state
RUN mkdir -p /opt/prefect/flows/data/geographic/city
RUN mkdir -p /opt/prefect/flows/data/geographic/zip_code

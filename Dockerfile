FROM apache/airflow:latest

USER root

# Update package lists and install build-essential without user interaction
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y build-essential \
    && apt-get clean

# Create a virtual environment directory
RUN mkdir /opt/airflow/venv

RUN chmod -R 777 /opt/airflow

# Switch to the airflow user
USER airflow

# Create a virtual environment within the directory
RUN python3 -m venv /opt/airflow/venv

# Activate the virtual environment
RUN source /opt/airflow/venv/bin/activate

# Copy requirements.txt
COPY requirements.txt /requirements.txt

# Install dependencies from requirements.txt within the virtual environment
RUN pip install -r /requirements.txt

CMD ["airflow", "standalone"]
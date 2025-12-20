FROM apache/airflow:2.10.2

# Install git as root
USER root
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Switch to airflow user for pip (required by Airflow image)
USER airflow

# Copy requirements and install them as airflow user
COPY --chown=airflow:0 requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# ✅ Make sure dbt has a profile available inside the container
# 1) Create the .dbt directory in the airflow home
RUN mkdir -p /home/airflow/.dbt

# 2) Copy your local profiles.yml into the container
#    (place profiles.yml next to this Dockerfile on your host)
COPY --chown=airflow:0 profiles.yml /home/airflow/.dbt/profiles.yml



FROM bitnami/spark:latest

USER root
RUN apt-get update && apt-get install -y wget 
RUN pip install pyspark sqlalchemy psycopg2-binary

# Set the WORKDIR to /app
WORKDIR /app

# Copy etl.py from the host's pyspark/ directory to /app/etl.py in the container
COPY etl/etl.py /app/etl.py

RUN mkdir -p /opt/jars \
    && wget -O /opt/jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Explicitly ensure /tmp/spark-temp exists and is writable
RUN mkdir -p /tmp/spark-temp && chmod 777 /tmp/spark-temp

# Ensure JAVA_TOOL_OPTIONS is set for all JVM processes to use /tmp/spark-temp
ENV JAVA_TOOL_OPTIONS="-Djava.io.tmpdir=/tmp/spark-temp"

CMD ["python", "etl.py"]
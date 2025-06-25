FROM bitnami/spark:latest

RUN pip install pyspark sqlalchemy psycopg2-binary

# Set the WORKDIR to /app
WORKDIR /app

# Copy etl.py from the host's pyspark/ directory to /app/etl.py in the container
COPY pyspark/etl.py /app/etl.py

# Copy the JDBC driver
COPY postgresql-42.6.0.jar /opt/jars/postgresql-42.6.0.jar

# Ensure JAVA_TOOL_OPTIONS is set for all JVM processes to use /tmp/spark-temp
ENV JAVA_TOOL_OPTIONS="-Djava.io.tmpdir=/tmp/spark-temp"

# Explicitly ensure /tmp/spark-temp exists and is writable
RUN mkdir -p /tmp/spark-temp && chmod 777 /tmp/spark-temp

CMD ["python", "etl.py"]
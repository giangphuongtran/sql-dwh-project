# Use the Bitnami Spark image as the base
FROM bitnami/spark:latest

# --- IMPORTANT: Temporarily switch to root to perform system-level changes ---
USER root

# Set environment variables for Spark's installation paths
ENV SPARK_HOME="/opt/bitnami/spark"
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# --- IMPORTANT: Add a user to fix "NullPointerException: invalid null input: name" ---
# This creates a 'spark' user with UID 1001 (a common non-root UID)
# It also sets their default shell to bash and ensures directories are correctly owned.
# The `chown` and `chmod` must also run as root.
RUN useradd -ms /bin/bash -u 1001 spark && \
    chown -R spark:root /opt/bitnami/spark && \
    chmod -R g+rwx /opt/bitnami/spark/logs

# Set the default user for running commands in the container to 'spark' for security
# All subsequent instructions and the container's runtime will be as this user.
USER spark
# --- End of root operations, now operating as 'spark' user ---

# Set the working directory inside the container
WORKDIR /app

# Copy your PySpark application code from your local './pyspark' folder to '/app' in the container
COPY ./pyspark /app

# Copy the PostgreSQL JDBC driver from your local project root to '/app/' in the container
COPY ./postgresql-42.6.0.jar /app/postgresql-42.6.0.jar

# No ENTRYPOINT or CMD here; we will define the full spark-submit command in docker-compose.yml
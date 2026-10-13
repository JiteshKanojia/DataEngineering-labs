# Build order (make build handles this):
#   1. de-labs-spark-base  (this file)
#   2. de-labs-jupyter     (FROM de-labs-spark-base)
#   3. de-labs-streamlit   (FROM de-labs-spark-base)

FROM apache/spark:4.2.0-scala2.13-java21-python3-ubuntu

USER root
ENV DEBIAN_FRONTEND=noninteractive \
    HOME=/root \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

# OS utilities (curl for healthchecks, procps for spark scripts)
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl procps iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Python packages used by the notebooks / dashboards
RUN pip3 install --no-cache-dir \
      "pyspark==4.2.0" \
      "delta-spark==4.4.1" \
      "pandas>=2.1" \
      "pyarrow>=14" \
      "numpy<2.1" \
      "plotly>=5.18" \
      "faker>=24"

# Ivy needs a writable home (the image ships with HOME=/nonexistent)
ENV SPARK_SUBMIT_OPTS="-Divy.home=/tmp/.ivy2"

# Prefetch Delta Lake runtime jars into $SPARK_HOME/jars so that
# `spark.sql.extensions=io.delta...` works with zero Ivy downloads at runtime.
# Only delta/unitycatalog jars are copied - Spark already ships newer
# hadoop/parquet/jackson/log4j versions, which we must not shadow.
RUN cd $SPARK_HOME \
    && bin/spark-submit --packages io.delta:delta-spark_4.2_2.13:4.4.1 \
         --master local[1] examples/src/main/python/pi.py > /tmp/ivy.log 2>&1 \
    && cp /tmp/.ivy2/jars/io.delta*.jar /tmp/.ivy2/jars/io.unitycatalog*.jar \
          $SPARK_HOME/jars/ \
    && ls $SPARK_HOME/jars | grep -iE 'delta|unitycatalog' \
    && rm -rf /tmp/.ivy2 /tmp/ivy.log

# Scratch space dirs (baked defaults; the docker-compose volume overrides these)
RUN mkdir -p /data/spark-events /data/warehouse /data/checkpoints

WORKDIR /workspace

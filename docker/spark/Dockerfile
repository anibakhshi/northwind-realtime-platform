FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

USER root

RUN python3 -m pip install \
    --no-cache-dir \
    psycopg2-binary==2.9.10

RUN mkdir -p \
    /opt/northwind/jobs \
    /opt/spark/checkpoints \
    && chown -R 185:0 \
    /opt/northwind \
    /opt/spark/checkpoints

USER 185

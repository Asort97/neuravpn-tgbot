# Dockerfile for HappyCat VPN PostgreSQL instance
# Usage example:
#   docker build -t happycat-postgres -f docker/postgres/Dockerfile .
#   docker run --name happycat-postgres -p 5432:5432 happycat-postgres

FROM postgres:15-alpine

ENV POSTGRES_USER=vpnbot \
    POSTGRES_PASSWORD=supersecret \
    POSTGRES_DB=vpnbot

COPY docker/postgres/init /docker-entrypoint-initdb.d

VOLUME ["/var/lib/postgresql/data"]

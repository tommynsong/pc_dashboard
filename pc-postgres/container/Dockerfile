FROM library/postgres
ENV POSTGRES_HOST_AUTH_METHOD=trust
LABEL org.opencontainers.image.authors="nospam.joe@gmail.com"
LABEL org.opencontainers.image.source="https://github.com/jtb75/pc_dashboard/tree/main/pc-postgres"
LABEL org.opencontainers.image.vendor="jtb75"
COPY files/init.sql /docker-entrypoint-initdb.d/
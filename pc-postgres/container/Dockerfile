FROM library/postgres
ENV POSTGRES_HOST_AUTH_METHOD=trust
LABEL org.opencontainers.image.authors="spamblackhole.tommy@gmail.com"
LABEL org.opencontainers.image.source="https://github.com/tommynsong/pc_dashboard/tree/main/pc-postgres"
LABEL org.opencontainers.image.vendor="focer"
COPY files/init.sql /docker-entrypoint-initdb.d/
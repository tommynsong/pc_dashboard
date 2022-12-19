CREATE DATABASE prisma;
CREATE USER prisma with encrypted password 'prisma';
GRANT ALL PRIVILEGES ON DATABASE prisma TO prisma;


\c prisma;
CREATE SCHEMA reporting;
GRANT ALL PRIVILEGES ON SCHEMA reporting TO prisma;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA reporting TO prisma;
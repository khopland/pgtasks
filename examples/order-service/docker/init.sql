-- Two roles, as a team with a DBA would have them: migrations run as the owner, the
-- application runs without DDL rights.
CREATE ROLE orders_owner LOGIN PASSWORD 'owner';
CREATE ROLE orders_app LOGIN PASSWORD 'app';
GRANT CREATE, CONNECT ON DATABASE orders TO orders_owner;
GRANT CONNECT ON DATABASE orders TO orders_app;

-- Whatever the owner creates later is usable by the application.
ALTER DEFAULT PRIVILEGES FOR ROLE orders_owner GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO orders_app;
ALTER DEFAULT PRIVILEGES FOR ROLE orders_owner GRANT USAGE, SELECT ON SEQUENCES TO orders_app;

-- The application's own tables live in schema "orders"; pgtasks gets its own schema from its migrations.
CREATE SCHEMA orders AUTHORIZATION orders_owner;
GRANT USAGE ON SCHEMA orders TO orders_app;
ALTER ROLE orders_owner SET search_path = orders;
ALTER ROLE orders_app SET search_path = orders;

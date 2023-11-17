CREATE SCHEMA IF NOT EXISTS users_directory;
SET SCHEMA 'users_directory';

DROP TABLE IF EXISTS users;

CREATE TABLE users(
    user_id serial PRIMARY KEY,
    username VARCHAR(25),
    email VARCHAR(25)
);
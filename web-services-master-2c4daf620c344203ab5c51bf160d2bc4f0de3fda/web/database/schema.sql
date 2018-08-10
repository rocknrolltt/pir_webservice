drop table if exists users;
create table users(
    api_key text primary key,
    name text not null
);
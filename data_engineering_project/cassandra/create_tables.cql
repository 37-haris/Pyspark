CREATE KEYSPACE IF NOT EXISTS spark_streaming
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS spark_streaming.random_names (
    full_name text PRIMARY KEY,
    gender    text,
    location  text,
    city      text,
    country   text,
    postcode  text,
    latitude  float,
    longitude float,
    email     text
);

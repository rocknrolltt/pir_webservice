-- Table: pir_transactions
DROP TABLE IF EXISTS pir_transactions;
CREATE TABLE pir_transactions
(
  trans_id character varying(36) NOT NULL,
  riskid character varying(12),
  street text,
  city text,
  state character varying(2),
  postal_code text,
  country character varying(3),
  latitude real,
  longitude real,
  hazard_list text,
  geocode_status integer,
  result_code text,
  trans_time text,
  remote_ip text,
  CONSTRAINT pir_transactions_pkey PRIMARY KEY (trans_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE pir_transactions
  OWNER TO pir_admin;
GRANT ALL ON TABLE pir_transactions TO pir_admin;
GRANT SELECT, UPDATE, DELETE, INSERT ON pir_transactions TO pir_write;
GRANT SELECT ON pir_transactions TO pir_read;

DROP TABLE IF EXISTS prometrix_addresses;
CREATE TABLE prometrix_addresses (
     riskid         VARCHAR(12) PRIMARY KEY,
     lownumber      text,
     highnumber     text, 
     predirection   text, 
     streetname     text NOT NULL,
     streettype     text, 
     postdirection  text, 
     city           text NOT NULL,
     postalcity     text,
     statecode      VARCHAR(2) NOT NULL,
     zip            VARCHAR(5) NOT NULL,
     zip4           NUMERIC(4),
     lat            REAL NOT NULL,
     long           REAL NOT NULL
);
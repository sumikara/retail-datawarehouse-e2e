
CREATE SEQUENCE IF NOT EXISTS 3nf.seq_nf_state_id START 1;
CREATE SEQUENCE IF NOT EXISTS 3nf.seq_nf_city_id START 1;
CREATE SEQUENCE IF NOT EXISTS 3nf.seq_nf_address_id START 1;


CREATE TABLE IF NOT EXISTS 3nf.nf_states (
    state_id        BIGINT PRIMARY KEY,
    state_src_id    VARCHAR(100) NOT NULL,
    state_name      VARCHAR(100) NOT NULL,
    source_system   VARCHAR(100) NOT NULL,
    source_table    VARCHAR(100) NOT NULL,
    insert_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_nf_states_src UNIQUE (state_src_id)
);

CREATE TABLE IF NOT EXISTS 3nf.nf_cities (
    city_id        BIGINT PRIMARY KEY,
    city_src_id    VARCHAR(150) NOT NULL,
    city_name      VARCHAR(100) NOT NULL,
    state_id       BIGINT NOT NULL,
    source_system  VARCHAR(100),
    source_table   VARCHAR(100),
    insert_dt      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_city_state
    FOREIGN KEY(state_id)
    REFERENCES 3nf.nf_states(state_id),
    CONSTRAINT uq_nf_cities_src UNIQUE (city_src_id)
);

CREATE TABLE IF NOT EXISTS 3nf.nf_addresses(
    address_id     BIGINT PRIMARY KEY,
    address_src_id VARCHAR(200) NOT NULL,
    zip_code       VARCHAR(30),
    city_id        BIGINT NOT NULL,
    source_system  VARCHAR(100),
    source_table   VARCHAR(100),
    insert_dt      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
CONSTRAINT fk_address_city
FOREIGN KEY(city_id) REFERENCES 3nf.nf_cities(city_id),
CONSTRAINT uq_nf_addresses_src UNIQUE (address_src_id)
);

CREATE INDEX IF NOT EXISTS idx_nf_states_src ON 3nf.nf_states (state_src_id);
CREATE INDEX IF NOT EXISTS idx_nf_cities_src ON 3nf.nf_cities(city_src_id);
CREATE INDEX IF NOT EXISTS idx_nf_addresses_src ON 3nf.nf_addresses(address_src_id);

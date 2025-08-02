CREATE TABLE IF NOT EXISTS spike_signals (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    spike_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    open_price NUMERIC NOT NULL,
    close_price NUMERIC NOT NULL,
    pct_change NUMERIC NOT NULL
);

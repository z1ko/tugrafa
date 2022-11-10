
CREATE KEYSPACE IF NOT EXISTS tugrafa
WITH REPLICATION = {
    'class' : 'SimpleStrategy',
    'replication_factor' : 1
};

/*
 * We store only the strictly necessary data for the queries
 */
CREATE TABLE IF NOT EXISTS tugrafa.swipes (
    card_id     TEXT,
    datetime    TIMESTAMP,
    poi         TEXT,

    PRIMARY KEY (card_id, datetime)
);
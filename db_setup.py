import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD

SQL_SCHEMA = """
-- Tabelle für Kerzen-Daten (OHLCV)
CREATE TABLE IF NOT EXISTS market_data (
    timestamp   TIMESTAMPTZ       NOT NULL,
    asset       TEXT              NOT NULL,
    timeframe   TEXT              NOT NULL,
    open        DOUBLE PRECISION  NOT NULL,
    high        DOUBLE PRECISION  NOT NULL,
    low         DOUBLE PRECISION  NOT NULL,
    close       DOUBLE PRECISION  NOT NULL,
    volume      DOUBLE PRECISION  NOT NULL,
    PRIMARY KEY (timestamp, asset, timeframe)
);
-- SELECT create_hypertable('market_data', 'timestamp'); -- This requires TimescaleDB extension, which might not be available by default. User needs to enable it.

-- Tabelle für Sentiment-Daten
CREATE TABLE IF NOT EXISTS sentiment_data (
    id              BIGSERIAL         PRIMARY KEY,
    timestamp_utc   TIMESTAMPTZ       NOT NULL,
    source          TEXT,
    headline        TEXT              NOT NULL,
    sentiment_score NUMERIC(3, 2)     NOT NULL
);

-- Tabelle für On-Chain-Transaktionen
CREATE TABLE IF NOT EXISTS onchain_transactions (
    tx_hash         TEXT              PRIMARY KEY,
    timestamp_utc   TIMESTAMPTZ       NOT NULL,
    wallet_monitored TEXT             NOT NULL,
    from_address    TEXT              NOT NULL,
    to_address      TEXT              NOT NULL,
    value_eth       NUMERIC           NOT NULL
);

-- NEU: Tabelle für generierte Handelssignale
CREATE TABLE IF NOT EXISTS generated_signals (
    signal_id           BIGSERIAL         PRIMARY KEY,
    timestamp_utc       TIMESTAMPTZ       NOT NULL,
    asset               TEXT              NOT NULL,
    signal_type         TEXT              NOT NULL, -- z.B. 'BUY'
    entry_price         DOUBLE PRECISION  NOT NULL,
    confidence_total    NUMERIC(3, 2)     NOT NULL,
    confidence_tech     NUMERIC(3, 2),
    confidence_sentiment NUMERIC(3, 2),
    confidence_onchain  NUMERIC(3, 2),
    triggering_factors  JSONB, -- Speichert die genauen Gründe als JSON
    take_profit_target  DOUBLE PRECISION,
    stop_loss_target    DOUBLE PRECISION
);
"""

def setup_database():
    """
    Verbindet sich mit der PostgreSQL-Datenbank und erstellt die notwendigen Tabellen.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()
        cur.execute(SQL_SCHEMA)
        conn.commit()
        cur.close()
        print("Datenbanktabellen erfolgreich erstellt oder aktualisiert.")
    except psycopg2.Error as e:
        print(f"Fehler beim Verbinden oder Erstellen der Datenbanktabellen: {e}")
        print("Bitte stellen Sie sicher, dass Ihre PostgreSQL-Datenbank läuft und die Zugangsdaten in config.py korrekt sind.")
        print("Beachten Sie, dass 'create_hypertable' (für TimescaleDB) auskommentiert ist. Aktivieren Sie es manuell, falls TimescaleDB verwendet wird.")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    setup_database()



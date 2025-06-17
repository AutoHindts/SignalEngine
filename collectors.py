import ccxt
import psycopg2
from datetime import datetime, timedelta
import time
import json
import requests
from openai import OpenAI

from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, ASSETS_TO_TRACK, TIMEFRAME, OPENAI_API_KEY, ETHERSCAN_API_KEY, WALLETS_TO_MONITOR

# Initialize OpenAI client
client = OpenAI(api_key=OPENAI_API_KEY)

def get_db_connection():
    """
    Stellt eine Verbindung zur PostgreSQL-Datenbank her.
    """
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def collect_market_data():
    """
    Sammelt OHLCV-Daten von Binance für die in config.py definierten Assets
    und speichert sie in der 'market_data'-Tabelle.
    """
    exchange = ccxt.binance()
    print(f"Sammle Marktdaten für Assets: {ASSETS_TO_TRACK} im Timeframe: {TIMEFRAME}")

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for asset in ASSETS_TO_TRACK:
            symbol = asset.replace('/', '') # ccxt uses 'BTCUSDT' instead of 'BTC/USDT'
            try:
                ohlcv = exchange.fetch_ohlcv(symbol, TIMEFRAME)
                for candle in ohlcv:
                    timestamp = datetime.fromtimestamp(candle[0] / 1000) # Convert ms to s
                    open_price = candle[1]
                    high_price = candle[2]
                    low_price = candle[3]
                    close_price = candle[4]
                    volume = candle[5]

                    cur.execute(
                        """
                        INSERT INTO market_data (timestamp, asset, timeframe, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (timestamp, asset, timeframe) DO NOTHING;
                        """,
                        (timestamp, asset, TIMEFRAME, open_price, high_price, low_price, close_price, volume)
                    )
                conn.commit()
                print(f"Marktdaten für {asset} erfolgreich gesammelt und gespeichert.")
            except Exception as e:
                print(f"Fehler beim Sammeln der Marktdaten für {asset}: {e}")
    except psycopg2.Error as e:
        print(f"Datenbankfehler beim Sammeln der Marktdaten: {e}")
    finally:
        if conn:
            conn.close()

def collect_sentiment_data():
    """
    Sammelt Sentiment-Daten aus Nachrichten (simuliert) und analysiert diese mit OpenAI.
    Speichert die Ergebnisse in der 'sentiment_data'-Tabelle.
    """
    # Simulierte Nachrichten-Headlines
    news_headlines = [
        "Bitcoin erreicht neues Allzeithoch nach starker Kaufwelle",
        "Krypto-Markt korrigiert nach Inflationsdaten",
        "Ethereum Upgrade erfolgreich abgeschlossen, positive Stimmung",
        "Regulierungsunsicherheit belastet Altcoins",
        "Großinvestoren zeigen Interesse an DeFi-Projekten"
    ]

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for headline in news_headlines:
            try:
                # Use OpenAI to get sentiment score
                response = client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": "You are a sentiment analysis bot. Analyze the sentiment of the given headline and return a score between -1 (very negative) and 1 (very positive). Only return the score as a float."},
                        {"role": "user", "content": f"Analyze the sentiment of this headline: '{headline}'"}
                    ],
                    temperature=0.0
                )
                sentiment_score_str = response.choices[0].message.content.strip()
                sentiment_score = float(sentiment_score_str)

                cur.execute(
                    """
                    INSERT INTO sentiment_data (timestamp_utc, source, headline, sentiment_score)
                    VALUES (%s, %s, %s, %s);
                    """,
                    (datetime.utcnow(), "Simulated News API", headline, sentiment_score)
                )
                conn.commit()
                print(f"Sentiment für '{headline}' erfolgreich gesammelt und gespeichert: {sentiment_score}")
            except Exception as e:
                print(f"Fehler beim Sammeln der Sentiment-Daten für '{headline}': {e}")
    except psycopg2.Error as e:
        print(f"Datenbankfehler beim Sammeln der Sentiment-Daten: {e}")
    finally:
        if conn:
            conn.close()

def collect_onchain_data():
    """
    Sammelt On-Chain-Transaktionen von Etherscan für die in config.py definierten Wallets
    und speichert sie in der 'onchain_transactions'-Tabelle.
    """
    print(f"Sammle On-Chain-Daten für Wallets: {WALLETS_TO_MONITOR}")

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for wallet_address in WALLETS_TO_MONITOR:
            if wallet_address == "0x...": # Skip placeholder wallets
                continue

            etherscan_url = f"https://api.etherscan.io/api?module=account&action=txlist&address={wallet_address}&startblock=0&endblock=99999999&sort=asc&apikey={ETHERSCAN_API_KEY}"
            try:
                response = requests.get(etherscan_url)
                response.raise_for_status() # Raise an exception for HTTP errors
                transactions = response.json()["result"]

                for tx in transactions:
                    tx_hash = tx["hash"]
                    timestamp_utc = datetime.fromtimestamp(int(tx["timeStamp"]))
                    from_address = tx["from"]
                    to_address = tx["to"]
                    value_eth = float(int(tx["value"]) / (10**18)) # Convert Wei to Eth

                    cur.execute(
                        """
                        INSERT INTO onchain_transactions (tx_hash, timestamp_utc, wallet_monitored, from_address, to_address, value_eth)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tx_hash) DO NOTHING;
                        """,
                        (tx_hash, timestamp_utc, wallet_address, from_address, to_address, value_eth)
                    )
                conn.commit()
                print(f"On-Chain-Daten für Wallet {wallet_address} erfolgreich gesammelt und gespeichert.")
            except requests.exceptions.RequestException as e:
                print(f"Fehler bei der Etherscan-API-Anfrage für Wallet {wallet_address}: {e}")
            except json.JSONDecodeError:
                print(f"Fehler beim Parsen der JSON-Antwort von Etherscan für Wallet {wallet_address}.")
            except Exception as e:
                print(f"Allgemeiner Fehler beim Sammeln der On-Chain-Daten für Wallet {wallet_address}: {e}")
    except psycopg2.Error as e:
        print(f"Datenbankfehler beim Sammeln der On-Chain-Daten: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Example usage (will require valid API keys and DB setup)
    # collect_market_data()
    # collect_sentiment_data()
    # collect_onchain_data()
    print("Collectors script executed. No data collection performed by default when run directly. Use main.py for orchestration.")



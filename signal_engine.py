import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, ASSETS_TO_TRACK, TIMEFRAME

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

def calculate_rsi(prices: pd.Series, period: int = 14) -> float:
    """
    Berechnet den Relative Strength Index (RSI).
    """
    delta = prices.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.rolling(window=period, min_periods=1).mean()
    avg_loss = loss.rolling(window=period, min_periods=1).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1] if not rsi.empty else 50.0 # Return last RSI, default to 50 if no data

def calculate_confidence_scores(asset: str) -> dict:
    """
    Berechnet technische, Sentiment- und On-Chain-Konfidenz-Scores für ein gegebenes Asset.
    """
    conn = None
    confidence_tech = 0.0
    confidence_sentiment = 0.0
    confidence_onchain = 0.0
    triggering_factors = {}

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # 1. Technische Konfidenz (RSI)
        cur.execute(
            """
            SELECT close FROM market_data
            WHERE asset = %s AND timeframe = %s
            ORDER BY timestamp DESC
            LIMIT 100; -- Genug Daten für RSI-Berechnung
            """,
            (asset, TIMEFRAME)
        )
        market_data = cur.fetchall()
        if market_data:
            prices = pd.Series([d[0] for d in reversed(market_data)]) # Need to reverse for chronological order
            rsi = calculate_rsi(prices)
            # Konvertiere RSI auf eine Skala von 0 bis 1
            # RSI 50 -> 0
            # RSI 20 -> 0.6 (starker Kauf)
            # RSI 80 -> -0.6 (starker Verkauf)
            confidence_tech = (50 - rsi) / 50.0 # Scale from -1 to 1, then adjust
            if rsi < 30: # Oversold, strong buy signal
                confidence_tech = (30 - rsi) / 30.0 * 0.8 + 0.2 # Scale 0-1, 0.2 is base for strong signal
            elif rsi > 70: # Overbought, strong sell signal
                confidence_tech = (70 - rsi) / 30.0 * 0.8 - 0.2 # Scale -1-0, -0.2 is base for strong signal
            else:
                confidence_tech = (50 - rsi) / 50.0 * 0.5 # Neutral range, smaller impact
            triggering_factors['rsi'] = rsi

        # 2. Sentiment Konfidenz (Durchschnitt der letzten 3 Stunden)
        three_hours_ago = datetime.utcnow() - timedelta(hours=3)
        cur.execute(
            """
            SELECT sentiment_score FROM sentiment_data
            WHERE timestamp_utc >= %s
            ORDER BY timestamp_utc DESC;
            """,
            (three_hours_ago,)
        )
        sentiment_data = cur.fetchall()
        if sentiment_data:
            sentiment_scores = [s[0] for s in sentiment_data]
            confidence_sentiment = float(np.mean(sentiment_scores)) # Already between -1 and 1
            triggering_factors['sentiment_avg_3h'] = confidence_sentiment

        # 3. On-Chain Konfidenz (Käufe von überwachten Wallets in den letzten 24 Stunden)
        twenty_four_hours_ago = datetime.utcnow() - timedelta(hours=24)
        cur.execute(
            """
            SELECT COUNT(*) FROM onchain_transactions
            WHERE timestamp_utc >= %s AND to_address IN (
                SELECT wallet_monitored FROM onchain_transactions WHERE wallet_monitored IS NOT NULL GROUP BY wallet_monitored
            ) AND value_eth > 0; -- Annahme: Kauf bedeutet Wert > 0
            """,
            (twenty_four_hours_ago,)
        )
        onchain_count = cur.fetchone()[0]
        if onchain_count > 0:
            confidence_onchain = 0.8
            triggering_factors['onchain_activity'] = 'significant_buy_activity'
        else:
            confidence_onchain = 0.0
            triggering_factors['onchain_activity'] = 'no_significant_buy_activity'

    except psycopg2.Error as e:
        print(f"Datenbankfehler beim Berechnen der Konfidenz-Scores für {asset}: {e}")
    except Exception as e:
        print(f"Fehler beim Berechnen der Konfidenz-Scores für {asset}: {e}")
    finally:
        if conn:
            conn.close()

    return {
        "confidence_tech": round(confidence_tech, 2),
        "confidence_sentiment": round(confidence_sentiment, 2),
        "confidence_onchain": round(confidence_onchain, 2),
        "triggering_factors": triggering_factors
    }

def generate_signals():
    """
    Generiert Handelssignale basierend auf den Konfidenz-Scores und speichert sie.
    """
    conn = None
    generated_signal_count = 0
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for asset in ASSETS_TO_TRACK:
            print(f"Generiere Signale für {asset}...")
            scores = calculate_confidence_scores(asset)
            confidence_tech = scores["confidence_tech"]
            confidence_sentiment = scores["confidence_sentiment"]
            confidence_onchain = scores["confidence_onchain"]
            triggering_factors = scores["triggering_factors"]

            # Algorithmus für Gesamtsignal
            confidence_total = (
                (confidence_tech * 0.4) +
                (confidence_sentiment * 0.3) +
                (confidence_onchain * 0.3)
            )
            confidence_total = round(confidence_total, 2)

            # Signal-Generierung
            if confidence_total > 0.75:
                signal_type = 'BUY'
                # Hole den aktuellen Preis für den Entry Price
                cur.execute(
                    """
                    SELECT close FROM market_data
                    WHERE asset = %s AND timeframe = %s
                    ORDER BY timestamp DESC
                    LIMIT 1;
                    """,
                    (asset, TIMEFRAME)
                )
                latest_price_row = cur.fetchone()
                if latest_price_row:
                    entry_price = latest_price_row[0]
                    take_profit_target = entry_price * 1.05
                    stop_loss_target = entry_price * 0.975

                    cur.execute(
                        """
                        INSERT INTO generated_signals (
                            timestamp_utc, asset, signal_type, entry_price, confidence_total,
                            confidence_tech, confidence_sentiment, confidence_onchain,
                            triggering_factors, take_profit_target, stop_loss_target
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """,
                        (
                            datetime.utcnow(), asset, signal_type, entry_price, confidence_total,
                            confidence_tech, confidence_sentiment, confidence_onchain,
                            json.dumps(triggering_factors), take_profit_target, stop_loss_target
                        )
                    )
                    conn.commit()
                    generated_signal_count += 1
                    print(f"BUY-Signal für {asset} generiert! Gesamtkondifenz: {confidence_total}")
                else:
                    print(f"Keine aktuellen Marktdaten für {asset} gefunden, kann kein Signal generieren.")
            else:
                print(f"Kein BUY-Signal für {asset} generiert. Gesamtkondifenz: {confidence_total}")

    except psycopg2.Error as e:
        print(f"Datenbankfehler beim Generieren der Signale: {e}")
    except Exception as e:
        print(f"Fehler beim Generieren der Signale: {e}")
    finally:
        if conn:
            conn.close()
    return generated_signal_count

if __name__ == "__main__":
    # Example usage (will require valid DB setup and some data in tables)
    # For testing, ensure you have some data in market_data, sentiment_data, and onchain_transactions
    # print(calculate_confidence_scores('BTC/USDT'))
    # generate_signals()
    print("Signal engine script executed. No signals generated by default when run directly. Use main.py for orchestration.")



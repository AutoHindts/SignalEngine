import psycopg2
import pandas as pd
from datetime import datetime, timedelta

from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD

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

def analyze_signals():
    """
    Bewertet die generierten Handelssignale basierend auf nachfolgenden Marktdaten.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Lade alle Signale
        cur.execute("SELECT * FROM generated_signals ORDER BY timestamp_utc ASC;")
        signals = cur.fetchall()
        signal_columns = [desc[0] for desc in cur.description]
        signals_df = pd.DataFrame(signals, columns=signal_columns)

        if signals_df.empty:
            print("Keine Signale zur Analyse gefunden.")
            return

        total_trades = len(signals_df)
        winning_trades = 0
        total_profit = 0.0
        total_loss = 0.0

        print(f"Analysiere {total_trades} Signale...")

        for index, signal in signals_df.iterrows():
            signal_id = signal["signal_id"]
            asset = signal["asset"]
            entry_price = signal["entry_price"]
            signal_timestamp = signal["timestamp_utc"]
            take_profit_target = signal["take_profit_target"]
            stop_loss_target = signal["stop_loss_target"]

            # Lade nachfolgende Marktdaten
            cur.execute(
                """
                SELECT timestamp, close FROM market_data
                WHERE asset = %s AND timestamp > %s
                ORDER BY timestamp ASC;
                """,
                (asset, signal_timestamp)
            )
            market_data = cur.fetchall()
            market_data_df = pd.DataFrame(market_data, columns=["timestamp", "close"])

            if market_data_df.empty:
                print(f"Keine nachfolgenden Marktdaten für Signal {signal_id} ({asset}) gefunden. Überspringe.")
                continue

            # Prüfe, ob Take Profit oder Stop Loss erreicht wurde
            target_reached = False
            for _, row in market_data_df.iterrows():
                current_price = row["close"]
                if current_price >= take_profit_target:
                    profit = take_profit_target - entry_price
                    total_profit += profit
                    winning_trades += 1
                    target_reached = True
                    # print(f"Signal {signal_id}: Take Profit erreicht! Profit: {profit:.2f}")
                    break
                elif current_price <= stop_loss_target:
                    loss = entry_price - stop_loss_target
                    total_loss += loss
                    target_reached = True
                    # print(f"Signal {signal_id}: Stop Loss erreicht! Verlust: {loss:.2f}")
                    break
            
            if not target_reached:
                # If neither target was reached, consider the last available price
                final_price = market_data_df["close"].iloc[-1]
                if final_price > entry_price:
                    profit = final_price - entry_price
                    total_profit += profit
                    winning_trades += 1
                    # print(f"Signal {signal_id}: Offener Gewinn: {profit:.2f}")
                else:
                    loss = entry_price - final_price
                    total_loss += loss
                    # print(f"Signal {signal_id}: Offener Verlust: {loss:.2f}")

        # Berechne Performance-Metriken
        win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
        profit_factor = total_profit / total_loss if total_loss > 0 else float("inf")

        print("\n--- Analyse-Ergebnisse ---")
        print(f"Anzahl der Trades: {total_trades}")
        print(f"Trefferquote (Win Rate): {win_rate:.2f}%")
        print(f"Profit-Faktor: {profit_factor:.2f}")
        print("-------------------------")

    except psycopg2.Error as e:
        print(f"Datenbankfehler bei der Signalanalyse: {e}")
    except Exception as e:
        print(f"Fehler bei der Signalanalyse: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Example usage (will require valid DB setup and some data in generated_signals and market_data)
    # analyze_signals()
    print("Analysis script executed. No analysis performed by default when run directly. Use main.py for orchestration.")



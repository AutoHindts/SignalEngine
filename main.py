from collectors import collect_market_data, collect_sentiment_data, collect_onchain_data
from signal_engine import generate_signals
from analysis import analyze_signals
from db_setup import setup_database

def main():
    """
    Orchestriert den gesamten Daten- und Signalgenerierungsprozess.
    """
    print("Starte den Setup der Datenbanktabellen...")
    setup_database()
    print("Datenbank-Setup abgeschlossen.")

    print("Starte Datensammlung...")
    collect_market_data()
    collect_sentiment_data()
    collect_onchain_data()
    print("Datensammlung abgeschlossen.")

    print("Starte Signalgenerierung...")
    generated_signal_count = generate_signals()
    print(f"Signalgenerierung abgeschlossen. {generated_signal_count} neue Signale generiert.")

    print("Starte Signalanalyse...")
    analyze_signals()
    print("Signalanalyse abgeschlossen.")

    print("Zyklus abgeschlossen.")

if __name__ == "__main__":
    main()



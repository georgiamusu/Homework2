import mysql.connector
import time


def get_db_connection():
    return mysql.connector.connect(
        host="user-db",
        user="root",
        password="rootpassword",
        database="user_db",
        port=3306
    )

def init_db():
    print("AVVIO INIZIALIZZAZIONE DATABASE...", flush=True)
    conn = None

    # Riprova per 60 volte (ogni 2 secondi)
    for i in range(60):
        try:
            print(f" Tentativo {i+1}/60 di connessione al DB...", flush=True)
            conn = get_db_connection()
            cursor = conn.cursor()

            # Creazione Tabella Utenti
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    email VARCHAR(120) PRIMARY KEY,
                    first_name VARCHAR(50),
                    last_name VARCHAR(50)
                )
            """)
            print(" Tabella 'users' verificata.", flush=True)

            # Creazione Tabella Log
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS request_log (
                    request_id VARCHAR(100) PRIMARY KEY,
                    response_json TEXT
                )
            """)
            print("Tabella 'request_log' verificata.", flush=True)

            conn.commit()
            print("DATABASE PRONTO E TABELLE CREATE!", flush=True)
            return True

        except mysql.connector.Error as err:
            print(f" DB non ancora pronto: {err}", flush=True)
            time.sleep(2) # Aspetta 2 secondi prima di riprovare
        except Exception as e:
            print(f" Errore generico: {e}", flush=True)
            time.sleep(2)
        finally:
            if conn and conn.is_connected():
                conn.close()

    print("IMPOSSIBILE CONNETTERSI AL DB DOPO 2 MINUTI.", flush=True)
    return False
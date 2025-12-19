import mysql.connector
import time

def get_db_connection():
    return mysql.connector.connect(
        host="data-db",         # Nome del container DB dati
        user="root",
        password="rootpassword",
        database="data_db",
        port=3306
    )

def init_db():
    conn = None
    print("Data Collector: Tentativo connessione DB")
    for i in range(10):
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # 1. Tabella Interessi (Chi segue cosa)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS interests (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    email VARCHAR(120) NOT NULL,
                    airport_code VARCHAR(10) NOT NULL,
                    high_value INT DEFAULT NULL,
                    low_value INT DEFAULT NULL,
                    UNIQUE(email, airport_code)
                )
            """)

            # 2. Tabella Voli (Storico dati scaricati)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS flight_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    airport_code VARCHAR(10),
                    query_time DATETIME,
                    arrivals_count INT DEFAULT 0,
                    departures_count INT DEFAULT 0
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS flights_detailed (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    icao24 VARCHAR(20),            
                    airport_monitorato VARCHAR(10),
                    icao_partenza VARCHAR(10),     
                    icao_arrivo VARCHAR(10),       
                    orario_partenza DATETIME,      
                    orario_arrivo DATETIME         
                )
            """)

            conn.commit()
            print("Data DB inizializzato!")
            return True
        except mysql.connector.Error as err:
            print(f"DB non pronto: {err}")
            time.sleep(3)
        finally:
            if conn and conn.is_connected(): conn.close()
    return False
import time
import json
import logging
import os
from datetime import datetime, timedelta
from flask import Flask, request, jsonify

from apscheduler.schedulers.background import BackgroundScheduler
import mysql.connector
import grpc
import pybreaker

from db import get_db_connection, init_db
from opensky_client import OpenSkyClient
import user_service_pb2
import user_service_pb2_grpc
from kafka import KafkaProducer


app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurazione Variabili d'Ambiente (con valori di default)
USER_MANAGER_HOST = os.getenv('USER_MANAGER_HOST', 'user-manager:50051')
OPENSKY_USER = os.getenv('OPEN_SKY_CLIENT_ID', 'giorgiamusumeci@hotmail.it-api-client')
OPENSKY_PASS = os.getenv('OPEN_SKY_CLIENT_SECRET', 'B4RGHwwL2JoFLtDvosDOmT1LssgSsXgZ')

# CONFIGURAZIONE CIRCUIT BREAKER
# Se fallisce 3 volte di fila, apre il circuito per 60 sec
circuit_breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=60)

# CONFIGURAZIONE KAFKA PRODUCER
producer = None

def get_kafka_producer():
    global producer
    if producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info(" Connesso a Kafka")
        except Exception as e:
            logger.warning(f" Kafka non ancora pronto: {e}")
            return None # Ritorna None se fallisce
    return producer # Ritorna il producer (nuovo o esistente)

# Inizializzazione Client OpenSky
opensky = OpenSkyClient(OPENSKY_USER, OPENSKY_PASS)

# CLIENT gRPC
def check_user_exists_grpc(email):
    try:
        with grpc.insecure_channel(USER_MANAGER_HOST) as channel:
            stub = user_service_pb2_grpc.UserManagerStub(channel)
            request_msg = user_service_pb2.UserRequest(email=email)
            response = stub.CheckUserExists(request_msg)
            return response.exists
    except grpc.RpcError as e:
        logger.error(f"Errore gRPC: {e}")
        return False

@circuit_breaker
def fetch_data_protected(airport_code):
    return opensky.get_arrival_stats(airport_code)

# SCHEDULER (Logica di Business)
def job_scarica_voli():
    logger.info(" Avvio job scaricamento voli...")
    conn = get_db_connection()
    if not conn:
        return

    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT DISTINCT airport_code FROM interests")
        airports = cursor.fetchall()

        kafka_prod = get_kafka_producer()

        for row in airports:
            code = row['airport_code']
            try:
                logger.info(f" Scarico lista voli per {code}...")

                flights = fetch_data_protected(code)

                if flights is not None:
                    arrivals_count = len(flights)
                    departures_count = 0

                    # 1. SALVARE I DETTAGLI
                    query_detail = """
                        INSERT INTO flights_detailed 
                        (icao24, airport_monitorato, icao_partenza, icao_arrivo, orario_partenza, orario_arrivo)
                        VALUES (%s, %s, %s, %s, FROM_UNIXTIME(%s), FROM_UNIXTIME(%s))
                    """

                    for f in flights:
                        icao = f.get('icao24')
                        dep_airport = f.get('estDepartureAirport')
                        arr_airport = f.get('estArrivalAirport')
                        dep_time = f.get('firstSeen') # Timestamp partenza
                        arr_time = f.get('lastSeen')  # Timestamp arrivo

                        cursor.execute(query_detail, (icao, code, dep_airport, arr_airport, dep_time, arr_time))

                    conn.commit()
                    logger.info(f"Salvati {arrivals_count} voli dettagliati nel DB.")

                    # 2. SALVARE I CONTEGGI
                    query_stats = """
                        INSERT INTO flight_data (airport_code, arrivals_count, departures_count, query_time)
                        VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(query_stats, (code, arrivals_count, departures_count, time.strftime('%Y-%m-%d %H:%M:%S')))
                    conn.commit()

                    # 3. KAFKA
                    if kafka_prod:
                        message = {
                            "airport_code": code,
                            "arrivals": arrivals_count,
                            "departures": departures_count,
                            "timestamp": time.time()
                        }
                        kafka_prod.send('flight_data', message)
                        logger.info(f"📨 Kafka notificato: {arrivals_count} arrivi per {code}")

            except pybreaker.CircuitBreakerError:
                logger.error(f" Circuit Breaker APERTO per {code}")
            except Exception as e:
                logger.error(f" Errore su {code}: {e}")

    except Exception as e:
        logger.error(f" Errore job: {e}")
    finally:
        cursor.close()
        conn.close()

# API REST
@app.route('/add_interest', methods=['POST'])
def add_interest():
    data = request.json
    email = data.get('email')
    airport_code = data.get('airport_code')

    high_value = data.get('high_value')
    low_value = data.get('low_value')

    if not email or not airport_code:
        return jsonify({"error": "Dati mancanti"}), 400

    # 1. Controllo validità soglie
    if high_value is not None and low_value is not None:
        if int(high_value) <= int(low_value):
            return jsonify({"error": "High value deve essere maggiore di Low value"}), 400

    # 2. Verifica Utente (gRPC)
    if not check_user_exists_grpc(email):
        return jsonify({"error": "Utente non trovato o errore comunicazione"}), 404

    # 3. Salva Interesse
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor()
        try:
            query = """
                    INSERT INTO interests (email, airport_code, high_value, low_value) 
                    VALUES (%s, %s, %s, %s)
                """
            cursor.execute(query, (email, airport_code, high_value, low_value))
            conn.commit()
            return jsonify({"message": f"Interesse aggiunto: {airport_code} (Soglie: {low_value}-{high_value})"}), 201
        except mysql.connector.Error as err:
            if err.errno == 1062: # Duplicate entry
                return jsonify({"message": "Interesse già presente"}), 200
            return jsonify({"error": str(err)}), 500
        finally:
            cursor.close()
            conn.close()
    return jsonify({"error": "Errore connessione DB"}), 500

@app.route('/stats/last/<airport_code>', methods=['GET'])
def get_last_stats(airport_code):
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "errore connessione db"}), 500

    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT * FROM flight_data 
            WHERE airport_code = %s 
            ORDER BY query_time DESC LIMIT 1
        """, (airport_code,))
        row = cursor.fetchone()

        if row:
            return jsonify(row), 200
        else:
            return jsonify({"message": "Nessun dato ancora scaricato"}), 404
    finally:
        cursor.close()
        conn.close()

@app.route('/stats/average/<code>/<int:days>', methods=['GET'])
def get_average_stats(code, days):
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "errore db"}), 500

    try:
        cursor = conn.cursor(dictionary=True)
        # Calcoliamo la data limite (oggi - X giorni)
        cutoff_date = datetime.now() - timedelta(days=days)

        # Query per fare la media
        cursor.execute("""
            SELECT AVG(arrivals_count) as media
            FROM flight_data 
            WHERE airport_code = %s AND query_time >= %s
        """, (code, cutoff_date))

        result = cursor.fetchone()

        # Se non ci sono dati, la media è 0
        media = result['media'] if result and result['media'] is not None else 0.0

        return jsonify({
            "airport": code,
            "days_analyzed": days,
            "average_arrivals": float(round(media, 2))
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

# --- AVVIO ---
if __name__ == '__main__':
    print("Attesa avvio servizi...", flush=True)
    time.sleep(10) # Aspetta che DB e Kafka siano pronti
    init_db()

    scheduler = BackgroundScheduler()
    scheduler.add_job(job_scarica_voli, 'interval', hours=12)
    scheduler.start()

    job_scarica_voli()

    print("Data Collector HW2 attivo sulla porta 5002")
    app.run(host='0.0.0.0', port=5002, debug=False)
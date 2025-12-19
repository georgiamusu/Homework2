import time
import json
import logging
from kafka import KafkaConsumer, KafkaProducer
import mysql.connector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = 'kafka:9092'
TOPIC_INPUT = 'flight_data'
TOPIC_OUTPUT = 'alerts'

def get_db_connection():
    return mysql.connector.connect(
        host='data-db',
        user='root',
        password='rootpassword',
        database='data_db'
    )

def main():
    logger.info("Alert System avviato. Attesa servizi...")
    time.sleep(15)

    # Consumer
    consumer = KafkaConsumer(
        TOPIC_INPUT,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        # CAMBIO IL NOME PER FORZARE LA RILETTURA DEI MESSAGGI VECCHI
        group_id='alert_groups_fix_final',
        auto_offset_reset='earliest'
    )

    # Producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    logger.info(f"Connesso a Kafka. Topic: {TOPIC_INPUT}")

    for message in consumer:
        try:
            data = message.value

            airport_code = data.get('airport_code')
            total_flights = data.get('arrivals')

            logger.info(f"Ricevuto dato: {airport_code} con {total_flights} voli. Controllo regole...")

            if not airport_code:
                continue

            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)

            query = "SELECT * FROM interests WHERE airport_code = %s"
            cursor.execute(query, (airport_code, ))
            rules = cursor.fetchall()

            if not rules:
                logger.info(f"Nessuna regola trovata nel DB per {airport_code}")

            for rule in rules:
                email = rule['email']
                high = rule['high_value']
                low = rule['low_value']
                alert_type = None

                # Controllo soglie
                if total_flights > high:
                    alert_type = "HIGH_TRAFFIC"
                elif total_flights < low:
                    alert_type = "LOW_TRAFFIC"

                if alert_type:
                    alert_msg = {
                        "email": email,
                        "airport": airport_code,
                        "flights": total_flights,
                        "alert_type": alert_type,
                        "message": f"Allarme {alert_type}: {total_flights} voli su {airport_code}"
                    }
                    producer.send(TOPIC_OUTPUT, alert_msg)
                    logger.info(f"ALLARME GENERATO per {email}: {alert_type}")

            cursor.close()
            conn.close()

        except Exception as e:
            logger.error(f"Errore nel ciclo: {e}")

if __name__ == "__main__":
    main()
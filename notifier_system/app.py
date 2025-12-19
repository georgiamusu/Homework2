import json
import smtplib
import logging
import time
from email.mime.text import MIMEText

from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

KAFKA_BROKER='kafka:9092'
TOPIC_ALERTS ='alerts'
SMTP_HOST = 'mailhog'
SMTP_PORT = 1025 #porta di mailhog

def send_email(alert_data):
    sender = "alertsystem@unict.com"
    receiver = alert_data['email']
    subject = f"ALERT VOLI:{alert_data['alert_type']} su {alert_data['airport']}"
    body = f""" 
    Il sistema ha rilevato una anomalia:
    
    Aeroporto: {alert_data['airport']}
    Voli Rilevati: {alert_data['flights']}
    Messaggio: {alert_data['message']}
    
    """
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = receiver

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.sendmail(sender,receiver,msg.as_string())
            logger.info(f"Email inviata a {receiver} per {alert_data['airport']}")
    except Exception as e:
        logger.error(f"Errore invio email: {e}")

def main():
    logger.info("Notifier System in avvio")
    time.sleep(20)

    consumer = KafkaConsumer(
        TOPIC_ALERTS,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='notifier_group'
    )

    logger.info("Notifier connesso e in ascolto su alerts")

    for message in consumer:
        alert_data = message.value
        logger.info(f"Ricevuto alert: {alert_data}")
        send_email(alert_data)

if __name__ == "__main__":
    main()
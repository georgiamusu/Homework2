import json
import os
import threading
import time
from concurrent import futures
import random

import grpc
from flask import Flask, request, jsonify, Response

from db import get_db_connection, init_db
import user_service_pb2
import user_service_pb2_grpc

from prometheus_client import Counter, Gauge, generate_latest

app = Flask(__name__)

#gRPC
class UserManagerService(user_service_pb2_grpc.UserManagerServicer):
    def CheckUserExists(self, request, context):
        exists= False
        conn = None
        try:
            conn= get_db_connection()
            cursor =conn.cursor()
            cursor.execute("SELECT 1 FROM users WHERE email= %s", (request.email,))
            if cursor.fetchone():
                exists= True
        except Exception as e:
            print (f"errore gRPC: {e}")
        finally :
            if conn and conn.is_connected(): conn.close()

            return user_service_pb2.UserResponse(exists=exists)

def serve_grpc():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_service_pb2_grpc.add_UserManagerServicer_to_server(UserManagerService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server gRPC attivo sulla porta 50051")
    server.wait_for_termination()

#monitoraggio
NODE_NAME = os.getenv('NODE_NAME', 'unknown-node')
SERVICE_NAME ='user-manager'

REQUEST_COUNT =Counter(
    'user_manager_requests_total',
    'Totale richieste ricevute dal servizio User Manager',
    ['method','endpoint', 'service', 'node']
)

#gauge -> misura il tempo di rispsota
OP_DURATION = Gauge(
    'user_manager_op_duration_seconds',
    'Durata delle operazioni interne',
    ['operation', 'service', 'node']
)



#API REST
@app.route('/register', methods=['POST'])
def register_user():
    data =request.json
    req_id = request.headers.get('X-Request-ID') or data.get('request_id')

    if not req_id:
        return jsonify({"error" : "manca request_id"}), 400

    conn =None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        #controllo log
        cursor.execute("SELECT response_json FROM request_log WHERE request_id = %s", (req_id,))
        existing = cursor.fetchone()
        if existing:
            return jsonify(json.loads(existing['response_json'])), 200

        #logica business
        email= data.get('email')
        cursor.execute("SELECT email FROM users WHERE email = %s", (email, )),
        if cursor.fetchone():
            resp = {"message": "email già registrata", "email": email}
            cursor.execute("INSERT INTO request_log (request_id, response_json) VALUES (%s, %s)", (req_id, json.dumps(resp)))
            conn.commit()
            return jsonify(resp), 200

        cursor.execute("INSERT INTO users (email, first_name, last_name) VALUES (%s, %s, %s)",
                       (email, data.get('first_name'), data.get('last_name')))

        resp = {"message": "Utente registrato", "email": email}

        #salvataggio log
        cursor.execute ("INSERT INTO request_log (request_id, response_json) VALUES (%s, %s)", (req_id, json.dumps(resp)))
        conn.commit()

        return jsonify(resp), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn and conn.is_connected(): conn.close()

@app.before_request
def before_request():
    request.start_time = time.time() #salvo orario inizio richiesta

@app.after_request
def after_request(response):
    latency = time.time() - request.start_time #calcolo quanto tempo è passato

    #aggiornamento counter
    REQUEST_COUNT.labels(
        method = request.method,
        endpoint = request.path,
        service = SERVICE_NAME,
        node = NODE_NAME
    ).inc()

    #aggiornamento gauge
    OP_DURATION.labels(
        operation = 'http_request',
        service = SERVICE_NAME,
        node = NODE_NAME
    ).set(latency)

    return response

@app.route('/metrics')
def metrics():
    return Response(generate_latest(), mimetype='text/plain')

@app.route('/')
def home():
    return jsonify({"message": "User Manager is running"})

@app.route('/db-test')
def db_test():
    with OP_DURATION.labels(operation='database_query', service=SERVICE_NAME, node=NODE_NAME).time():
        # Qui simuliamo un ritardo del DB
        time.sleep(random.uniform(0.1, 0.5))
    return jsonify({"status": "DB query simulated"})


if __name__ == '__main__':
    print("AVVIO USER MANAGER", flush=True)
    init_db()

    t = threading.Thread(target=serve_grpc)
    t.daemon = True
    t.start()

    app.run(host='0.0.0.0', port=5001, debug=False)

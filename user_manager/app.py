import json
import threading
from concurrent import futures


import grpc
from flask import Flask, request, jsonify

from db import get_db_connection, init_db
import user_service_pb2
import user_service_pb2_grpc

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

#main
if __name__ == '__main__':
    print("AVVIO USER MANAGER", flush=True)
    init_db()

    t = threading.Thread(target=serve_grpc)
    t.daemon = True
    t.start()

    app.run(host='0.0.0.0', port=5001, debug=False)

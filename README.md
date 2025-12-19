# Homework 2 

Il sistema rappresenta l'evoluzione dell'Homework 1. Oltre alla gestione utente e alla raccolta dati, implementa ora un'architettura **Event-Driven** basata su **Apache Kafka** per il monitoraggio in tempo reale e l'invio di notifiche di allarme via email.

## Architettura del Sistema

Il sistema è composto da **8 container** orchestrati tramite Docker Compose:

1.  **User Manager Service** (`user-manager`): Gestisce la registrazione utenti. Implementa logica REST per il client e gRPC Server per la verifica interna.
2.  **Data Collector Service** (`data-collector`): Gestisce la raccolta dati. Scarica i dati da OpenSky (con Circuit Breaker) e li pubblica su Kafka (Producer).
3.  **User DB** (`user-db`): Database MySQL per dati anagrafici e log di idempotenza.
4.  **Data DB** (`data-db`): Database MySQL per memorizzare interessi, regole di allarme e storico voli.
5.  **Kafka** (`kafka`): Message Broker per i topic `flight_data` e `alerts`.
6.  **Alert System** (`alert-system`): Consuma i dati di volo, verifica le regole nel DB e genera allarmi.
7.  **Notifier System** (`notifier-system`): Consuma gli allarmi e invia email.
8.  **MailHog** (`mailhog`): Server SMTP fittizio per visualizzare le email in locale.

## 🚀 Istruzioni per l'Avvio (Deploy)

1.  **Avviare il sistema (build automatica):**
    ```bash
    docker compose up --build -d
    ```

2.  **Verificare lo stato dei container:**
    ```bash
    docker compose ps
    ```

3.  **Accedere a MailHog:**
    Aprire il browser su: [http://localhost:8025](http://localhost:8025)

> **Nota:** All'avvio, il Data Collector forza un primo scaricamento dati immediato per dimostrazione, dopodiché prosegue con cicli di 12 ore come da specifiche.

## API (Documentazione)

### 1. User Manager (Porta 5001)

#### Registrazione Utente (At-Most-Once)
* **Endpoint:** `POST http://localhost:5001/register`
* **Descrizione:** Registra un utente implementando la politica **At-Most-Once** tramite `request_id`.
* **Body (JSON):**
    ```json
    {
      "email": "giorgia@test.com",
      "first_name": "Giorgia",
      "last_name": "Musumeci",
      "request_id": "req-univoco-001"
    }
    ```

### 2. Data Collector (Porta 5002)

#### Aggiungi Interesse e Soglie
* **Endpoint:** `POST http://localhost:5002/add_interest`
* **Descrizione:** Aggiunge un aeroporto da monitorare e le soglie di allarme (verifica prima l'utente via gRPC).
* **Body (JSON):**
    ```json
    {
      "email": "giorgia@test.com",
      "airport_code": "KJFK",
      "high_value": 10000,
      "low_value": 5000
    }
    ```

#### Statistiche: Ultimo Volo
* **Endpoint:** `GET /stats/last/<codice_aeroporto>`
* **Esempio:** `http://localhost:5002/stats/last/KJFK`
* **Descrizione:** Restituisce i dati dell'ultimo scaricamento effettuato per quell'aeroporto.

#### Statistiche: Media
* **Endpoint:** `GET /stats/average/<codice_aeroporto>/<giorni>`
* **Esempio:** `http://localhost:5002/stats/average/KJFK/7`
* **Descrizione:** Calcola la media dei voli in arrivo negli ultimi X giorni.

## Scenari di Test

Per verificare il corretto funzionamento dell'intero sistema, eseguire i seguenti passaggi:

### 1. Test Registrazione e Alerting (Flusso Completo)
1.  **Registra un utente:** Invia una POST a `/register` (vedi body sopra).
2.  **Aggiungi un interesse:** Invia una POST a `/add_interest` con soglie che verranno violate (es. `low_value: 10000` per forzare un alert LOW_TRAFFIC).
3.  **Scarica i voli:** Attendi il ciclo automatico.
4.  **Verifica Alert:** Vai su [http://localhost:8025](http://localhost:8025) e controlla di aver ricevuto l'email di allarme.

### 2. Test delle Statistiche (API REST)
Dopo aver scaricato i dati, è possibile interrogare le API di statistica:

* **Verifica Ultimo Volo:**
  Apri nel browser: `http://localhost:5002/stats/last/KJFK`
  *Risultato atteso:* Un JSON contenente i dettagli dell'ultimo rilevamento (timestamp, numero arrivi/partenze).

* **Verifica Media Voli:**
  Apri nel browser: `http://localhost:5002/stats/average/KJFK/3`
  *Risultato atteso:* Un JSON con la media calcolata sugli ultimi 3 giorni.
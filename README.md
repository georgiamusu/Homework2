# Homework 3

Questo progetto rappresenta l'evoluzione finale del sistema di monitoraggio del traffico aereo. L'architettura è stata migrata da un'orchestrazione locale (Docker Compose) a un sistema distribuito **Cloud Native** basato su **Kubernetes** (cluster locale Kind).

Oltre all'architettura a microservizi **Event-Driven** basata su Kafka, è stato introdotto un livello di **Observability** tramite **Prometheus** per il monitoraggio "White-box" delle metriche applicative e infrastrutturali.

## Architettura del Sistema

Il sistema è deployato su Kubernetes e organizzato nei seguenti layer logici (Manifest YAML):

1.  **Infrastructure Layer** (`infrastructure.yaml`):
    * **MySQL:** Database dedicati per User Service e Data Collector.
    * **Apache Kafka:** Message Broker per i topic `flight_data` e `alerts`.
2.  **Application Layer** (`apps.yaml`):
    * **User Manager:** Gestisce utenti (REST) e verifiche interne (gRPC).
    * **Data Collector:** Raccoglie dati da OpenSky e produce messaggi su Kafka.
3.  **Alerting Layer** (`alert-system.yaml`):
    * **Alert System:** Consumer Kafka che analizza i dati di volo e genera allarmi se le soglie vengono superate.
4.  **Notification Layer** (`notifier.yaml`):
    * **Notifier System:** Consumer Kafka che invia le email finali.
    * **MailHog:** Server SMTP di test con interfaccia Web per visualizzare le email inviate.
5.  **Monitoring Layer** (`prometheus.yaml`):
    * **Prometheus:** Server che esegue lo scraping delle metriche dai microservizi.

---

## Istruzioni per il Deployment

### Prerequisiti
* Docker
* Kind (Kubernetes in Docker)
* Kubectl

### 1. Avvio del Cluster e dei Servizi
Eseguire i manifest nel seguente ordine per garantire la disponibilità delle dipendenze:

```bash
# 1. Avviare Infrastruttura (DB e Kafka)
kubectl apply -f k8s/infrastructure.yaml

# 2. Avviare Applicazioni Core (User e Data)
kubectl apply -f k8s/apps.yaml

# 3. Avviare Alert System
kubectl apply -f k8s/alert-system.yaml

# 4. Avviare Notifier e MailHog
kubectl apply -f k8s/notifier.yaml

# 5. Avviare Prometheus
kubectl apply -f k8s/prometheus.yaml
```
### 2. Verifica dello stato
Attendere che tutti i Pod siano in stato Running:
```bash
kubectl get pods 
```
### 3. Accesso ai Servizi (Port Forwarding)
Poiché il cluster Kind è isolato, per accedere alle API e alle Dahboard da localhost è necessario attivare il port-forwarding:
```bash
# Terminale A: User Manager (API Utenti)
kubectl port-forward service/user-manager 5001:5001

# Terminale B: Data Collector (API Dati)
kubectl port-forward service/data-collector 5002:5002

# Terminale C: MailHog (Visualizzazione Email)
kubectl port-forward service/mailhog 8025:8025

# Terminale D: Prometheus (Dashboard Metriche)
kubectl port-forward service/prometheus 9090:9090
```


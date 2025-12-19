import requests
import time
import logging

logger = logging.getLogger(__name__)

class OpenSkyClient:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.token_expiry = 0
        self.token_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
        self.api_url = "https://opensky-network.org/api"

    def _get_token(self):
        if self.token and time.time() < self.token_expiry:
            return self.token

        logger.info(" Richiesta nuovo Token OpenSky...")
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        try:
            response = requests.post(self.token_url, data=payload)
            if response.status_code == 200:
                data = response.json()
                self.token = data['access_token']
                # Scade un po' prima per sicurezza
                self.token_expiry = time.time() + data.get('expires_in', 1800) - 60
                logger.info(" Token ottenuto con successo")
                return self.token
            else:
                logger.error(f" Errore Token: {response.text}")
                response.raise_for_status()
        except Exception as e:
            logger.error(f"Errore connessione auth: {e}")
            raise e

    def get_arrival_stats(self, airport_code):
        """
        Scarica la LISTA COMPLETA dei voli usando il Token Bearer.
        """
        end_time = int(time.time())
        begin_time = end_time - (12 * 3600)

        try:
            # 1. Ottieni il token valido
            token = self._get_token()

            # 2. Prepara la richiesta autenticata
            headers = {
                "Authorization": f"Bearer {token}"
            }

            url_arrival = f"{self.api_url}/flights/arrival"
            params = {
                'airport': airport_code,
                'begin': begin_time,
                'end': end_time
            }

            logger.info(f" Scarico voli per {airport_code} (Auth: Bearer Token)...")
            response = requests.get(url_arrival, headers=headers, params=params, timeout=10)

            if response.status_code == 200:
                flights_list = response.json()
                logger.info(f" Scaricati {len(flights_list)} voli.")
                return flights_list

            elif response.status_code == 404:
                logger.warning(f"Nessun volo trovato per: {airport_code}")
                return []
            else:
                logger.error(f"Errore API ({response.status_code}): {response.text}")
                response.raise_for_status()

        except requests.exceptions.RequestException as e:
            logger.error(f"Eccezione OpenSky: {e}")
            raise e
FROM python:3.9-slim
WORKDIR /app

#installo le dipendenze
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

#copio il codice
COPY . .

#avvio app
CMD ["python", "-u", "app.py"]

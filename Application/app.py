from flask import Flask, render_template, request, redirect, url_for
from flask_socketio import SocketIO
from kafka import KafkaProducer
from json import dumps
import pandas as pd
from time import sleep
import os

app = Flask(__name__)
socketio = SocketIO(app)

# Kafka broker address
bootstrap_servers = ['localhost:9092']

# Kafka topic to produce messages to
topic_name = 'Twitter'

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

# Function to ensure the existence of the 'uploads' directory
def ensure_uploads_dir():
    uploads_dir = 'uploads'
    if not os.path.exists(uploads_dir):
        os.makedirs(uploads_dir)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/', methods=['POST'])
def upload_csv():
    ensure_uploads_dir()
    if 'csv_file' in request.files:
        csv_file = request.files['csv_file']
        if csv_file.filename == '':
            return redirect(request.url)
        if csv_file:
            # Read the content of the uploaded CSV file
            df = pd.read_csv(csv_file)
            # Send each row of the CSV file to Kafka producer
            for _, row in df.iterrows():
                send_to_kafka(row.to_dict())
                sleep(1)  # Introduce a delay between producing messages
            return "CSV file uploaded successfully and sent to Kafka producer."
    return redirect(request.url)

def send_to_kafka(data):
    if isinstance(data, pd.Series):
        # Convert Series to dictionary
        message = data.to_dict()
        # Produce the message to Kafka topic
        producer.send(topic_name, value=message)
        print(f"Data sent to Kafka topic: {message}")
    else:
        # Produce the message to Kafka topic
        producer.send(topic_name, value={'tweet': data})
        print(f"Tweet sent to Kafka topic: {data}")
    producer.flush()  # Ensure that all messages are sent before the delay

@socketio.on('message')
def handle_message(message):
    print('received message: ' + message)

if __name__ == "__main__":
    socketio.run(app, debug=True)

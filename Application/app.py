from flask import Flask, render_template, request, redirect, url_for
from flask_socketio import SocketIO
from kafka import KafkaProducer
from json import dumps
import pandas as pd
from time import sleep
import os
from pymongo import MongoClient


app = Flask(__name__)
socketio = SocketIO(app)

# Kafka broker address
bootstrap_servers = ['localhost:9092']

# Kafka topic to produce messages to
topic_name = 'Twitter'

# MongoDB connection
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['Twitter_data']
collection = db['Test']

# Initialize data variable at module level
data = None

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
    # Check if data is not None and not empty
    if data is not None and not data.empty:
        return render_template('index.html', data=data.to_dict(orient='records'))
    else:
        return render_template('index.html', data=None)


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
            # Save the data to a global variable
            global data
            data = df
            # Redirect to the tables route to display the data
            return render_template('index.html', data=data.to_dict(orient='records'))
    return redirect(request.url)



@app.route('/tables')
def show_tables():
    # Check if data is not None and not empty
    if data is not None and not data.empty:
        return render_template('tables.html', data=data.to_dict(orient='records'))
    else:
        return render_template('tables.html', data=None)

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

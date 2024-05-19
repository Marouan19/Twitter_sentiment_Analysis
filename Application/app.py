from flask import Flask, render_template, request, redirect, url_for
from flask_socketio import SocketIO, emit
from kafka import KafkaProducer
from json import dumps
import pandas as pd
from time import sleep
import os
from pymongo import MongoClient
import threading
import hashlib
from bson import ObjectId

app = Flask(__name__)
socketio = SocketIO(app)

# Kafka broker address
bootstrap_servers = ['localhost:9092']

# Kafka topic to produce messages to
topic_name = 'my_topic'

# MongoDB connection
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['Twitter_data']
collection = db['Test']

# Initialize data variable at module level
data = None

# Initialize data_mongodb variable at module level
data_mongodb = None

# Extract data from MongoDB and save to global variable
def load_data_from_mongodb():
    global data_mongodb
    cursor = collection.find()
    data_list = list(cursor)
    data_mongodb = pd.DataFrame(data_list)

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
            for _, row in df.iterrows():
                send_to_kafka(row.to_dict())
            # Redirect to the tables route to display the data
            return render_template('index.html', data=data.to_dict(orient='records'))
    return redirect(request.url)

def convert_objectid_to_str(data):
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                for key, value in item.items():
                    if isinstance(value, ObjectId):
                        item[key] = str(value)
    elif isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, ObjectId):
                data[key] = str(value)
    return data

@app.route('/charts')
def charts():
    load_data_from_mongodb()
    if data_mongodb is not None and not data_mongodb.empty:
        data_mongodb_records = data_mongodb.to_dict(orient='records')
        data_mongodb_records = convert_objectid_to_str(data_mongodb_records)
        return render_template('charts.html', data_mongodb=data_mongodb_records)
    else:
        return render_template('charts.html', data_mongodb=None)

@app.route('/text')
def text():
    return render_template('text.html', data=None)

@app.route('/layout-static')
def layout_static():
    load_data_from_mongodb()
    # Check if data_mongodb is not None and not empty
    if data_mongodb is not None and not data_mongodb.empty:
        return render_template('layout-static.html', data_mongodb=data_mongodb.to_dict(orient='records'))
    else:
        return render_template('layout-static.html', data_mongodb=None)



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

def background_thread():
    last_hash = None
    while True:
        load_data_from_mongodb()
        current_hash = hashlib.md5(pd.util.hash_pandas_object(data_mongodb).values).hexdigest()
        if current_hash != last_hash:
            last_hash = current_hash
            socketio.emit('update_data', {'data_mongodb': data_mongodb.to_dict(orient='records')}, namespace='/charts')
        sleep(10)  # Adjust sleep interval based on your needs

if __name__ == "__main__":
    # Start the background thread when the server starts
    thread = threading.Thread(target=background_thread)
    thread.start()
    socketio.run(app, debug=True)

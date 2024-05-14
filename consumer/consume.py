from kafka import KafkaConsumer
from json import loads
from pymongo import MongoClient
from flask_socketio import SocketIO, send
from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)
socketio = SocketIO(app)

# Kafka broker address
bootstrap_servers = ['localhost:9092']

# Kafka topic to consume messages from
topic_name = 'twitter'

# MongoDB connection
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['Twitter_data']
collection = db['Test']

# Create Kafka consumer
consumer = KafkaConsumer(topic_name,
                         group_id='twitter_consumer_group',
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         value_deserializer=lambda x: loads(x.decode('utf-8')))

def save_to_mongodb():
    for message in consumer:
        try:
            tweet_data = message.value
            if isinstance(tweet_data, dict) and 'tweet' in tweet_data:
                tweet_content = tweet_data['tweet']
                tweet_dict = {
                    'id': tweet_content.get('id'),
                    'branch': tweet_content.get('branch'),
                    'sentiment': tweet_content.get('sentiment'),
                    'tweet': tweet_content.get('tweet')
                }
                collection.insert_one(tweet_dict)
                print(f"Saved tweet to MongoDB: {tweet_dict}")
                # Emit the received data to the client side
                socketio.emit('tweet_data', tweet_dict)
            else:
                print(f"Received message is not in the expected format: {tweet_data}")
        except Exception as e:
            print(f"Error processing message: {e}")

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == "__main__":
    socketio.run(app, debug=True)

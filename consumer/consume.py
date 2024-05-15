from kafka import KafkaConsumer
from json import loads
from pymongo import MongoClient
from pyspark.ml import PipelineModel
import re
from pyspark.ml.feature import StopWordsRemover, Tokenizer
from pyspark.sql import SparkSession

# Load the pre-trained logistic regression model
model = PipelineModel.load("lrmodel")

# MongoDB connection
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['Twitter_data']
collection = db['Test']

# Create Kafka consumer
consumer = KafkaConsumer('Twitter',
                         group_id='twitter_consumer_group',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         value_deserializer=lambda x: loads(x.decode('utf-8')))

spark = SparkSession.builder.appName("Preprocessing").getOrCreate()


# Define UDFs for preprocessing steps
def normalize_text(text):
    return text.lower()


def remove_html_tags(text):
    return re.sub(r'<.*?>', '', text)


def remove_urls(text):
    return re.sub(r'http\S+|www\S+', '', text)


def remove_numbers(text):
    return re.sub(r'\d+', '', text)


def remove_punctuation(text):
    return re.sub(r'[^\w\s]', '', text)


def remove_emojis(text):
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U00002500-\U00002BEF"  # chinese char
                               u"\U00002702-\U000027B0"
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               u"\U0001f926-\U0001f937"
                               u"\U00010000-\U0010ffff"
                               u"\u2640-\u2642"
                               u"\u2600-\u2B55"
                               u"\u200d"
                               u"\u23cf"
                               u"\u23e9"
                               u"\u231a"
                               u"\ufe0f"  # dingbats
                               u"\u3030"
                               "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)


# Define functions for preprocessing steps
def preprocess_text(tweet_text):
    text = tweet_text['tweet']  # Extract tweet text
    text = normalize_text(text)
    text = remove_html_tags(text)
    text = remove_urls(text)
    text = remove_numbers(text)
    text = remove_punctuation(text)
    text = remove_emojis(text)
    return text


# Define a function to tokenize and remove stopwords
def tokenize_and_remove_stopwords(text):
    tokenizer = Tokenizer(inputCol="tweet", outputCol="tokens")
    tokenized_df = tokenizer.transform(text)

    remover = StopWordsRemover(inputCol="tokens", outputCol="filtered_tokens")
    df = remover.transform(tokenized_df).select("filtered_tokens")

    return df


# Define a function to predict sentiment
def predict_sentiment(tweet_text):
    # Preprocess the tweet
    preprocessed_tweet = preprocess_text(tweet_text)

    # Create DataFrame with a single column 'tweet' containing the preprocessed tweet text
    df = spark.createDataFrame([(preprocessed_tweet,)], ['tweet'])

    # Tokenize and remove stopwords
    tokenized_df = tokenize_and_remove_stopwords(df)

    # Predict sentiment using the loaded model
    prediction = model.transform(tokenized_df)

    # Extract sentiment prediction from the result
    sentiment = prediction.select('prediction').collect()[0][0]

    return sentiment


# Modify the save_to_mongodb function to preprocess the tweet data, predict sentiment, and save the results to MongoDB
def save_to_mongodb():
    for message in consumer:
        try:
            tweet_data = message.value
            if isinstance(tweet_data, dict) and 'tweet' in tweet_data:
                tweet_content = tweet_data['tweet']  # Extract tweet content
                print(f"Tweet content: {tweet_content}")  # Add this line to print tweet content

                # Extract tweet_id, topic, and sentiment
                tweet_id = tweet_content.get('tweet_id')
                topic = tweet_content.get('topic')
                sentiment = tweet_content.get('sentiment')

                # Predict sentiment
                sentiment_prediction = predict_sentiment(tweet_content)

                # Extract tweet text
                tweet_text = tweet_content.get('tweet')

                # Save to MongoDB
                tweet_dict = {
                    'tweet_id': tweet_id,
                    'topic': topic,
                    'sentiment': sentiment,
                    'tweet': tweet_text,
                    'predicted_sentiment': sentiment_prediction
                }
                collection.insert_one(tweet_dict)
                print(f"Saved tweet to MongoDB: {tweet_dict}")
            else:
                print(f"Received message is not in the expected format: {tweet_data}")
        except Exception as e:
            print(f"Error processing message: {e}")

if __name__ == "__main__":
    save_to_mongodb()
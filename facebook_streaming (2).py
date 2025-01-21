from pyspark.sql import SparkSession
import requests
import json
import time
import logging

# Configure the logging system
logging.basicConfig(level=logging.INFO)  # Set the logging level
logger = logging.getLogger(__name__)  # Create a logger instance

def fetch_facebook_data(access_token, endpoint, params=None):
    url = f"https://graph.facebook.com/v12.0/{endpoint}"
    params = params or {}
    params['access_token'] = access_token
    logger.info("Fetching data from Facebook API: %s", url)  # Log the API call
    
    try:
        response = requests.get(url, params=params)
        logger.info("Received response with status code: %d", response.status_code)  # Log the response status
        
        # Check for errors in the response
        if response.status_code != 200:
            logger.error("Error fetching data: %s", response.json())  # Log error details
            return {}
        
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error("Request failed: %s", e)  # Log request failure
        return {}
    except json.JSONDecodeError:
        logger.error("Failed to decode JSON response")  # Log JSON decoding error
        return {}

# Function to simulate streaming data by continuously fetching from Facebook API
def fetch_and_stream_data(spark, access_token):
    user_id = ''  # Update with your user ID
    while True:
        logger.info("Fetching posts for user ID: %s", user_id)  # Log the user ID being queried
        posts = fetch_facebook_data(access_token, f"{user_id}/posts")
        
        data = [json.dumps(post) for post in posts.get('data', [])]
        
        if data:
            logger.info("Received %d posts", len(data))  # Log the number of posts received
            df = spark.read.json(spark.sparkContext.parallelize(data))
            df.write.format("console").option("truncate", "false").save()
        else:
            logger.info("No new posts received.")  # Log if no new posts are available
        
        time.sleep(30)  # Adjust the sleep interval as needed

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("FacebookStreaming") \
        .getOrCreate()

    access_token = ''# Update with your token

    # Start the data fetching and streaming process
    logger.info("Starting the Facebook streaming process...")
    fetch_and_stream_data(spark, access_token)

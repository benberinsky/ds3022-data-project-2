from airflow.decorators import dag, task
from datetime import datetime
import requests
import boto3
import time

# Defining necessary vars
url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/tfu5hw"
uva_id = "tfu5hw"
platform = "airflow"

# Defining parameters(manually run)
@dag(
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
    catchup=False,
    tags=['assignment']
)

def taskflow_dag():    
    @task
    # Makes api request to get payload from api
    def api_request():
        try:
            print(f"Making request to: {url}")
            response = requests.post(url, timeout=30)
            response.raise_for_status()
            payload = response.json()
            
            print(f"API Response Status: {response.status_code}")
            print(f"Payload received: {payload}")
            
            # Validate payload has required keys
            if 'sqs_url' not in payload:
                raise ValueError(f"Missing 'sqs_url' in payload. Got keys: {list(payload.keys())}")
            
            return payload
            
        # error handling for specific errors
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            raise
        except ValueError as e:
            print(f"Payload validation failed: {e}")
            raise
    
    #Collect messages from SQS queue
    @task
    def collect_messages(payload):
        print(f"Starting message collection with payload: {payload}")
        
        # Initialize SQS client 
        sqs = boto3.client('sqs', region_name='us-east-1')
        word_dict = {}
        
        # Verify we have the sqs_url
        if 'sqs_url' not in payload:
            raise ValueError(f"Payload missing 'sqs_url'. Keys present: {list(payload.keys())}")
        
        queue_url = payload["sqs_url"]
        print(f"Using queue URL: {queue_url}")
        
        # getting queue attributes
        try:
            attr_response = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['All']
            )
        except Exception as e:
            print(f"Error getting queue attributes: {e}")
            raise
    
        if "Attributes" not in attr_response:
            print("Attributes field not found in queue response")
            return word_dict
        
        # getting message counts, converting to ints
        visible_messages = int(attr_response["Attributes"]["ApproximateNumberOfMessages"])
        invisible_messages = int(attr_response["Attributes"]["ApproximateNumberOfMessagesNotVisible"])
        delayed_messages = int(attr_response["Attributes"]["ApproximateNumberOfMessagesDelayed"])
        
        print(f"Initial counts - Visible: {visible_messages}, Invisible: {invisible_messages}, Delayed: {delayed_messages}")
        
        #making sure loop doesn't run repeatedly if no messages are processed
        max_iterations = 100 
        iterations = 0
        
        # checking there are messages and max iterations hasn't been hit
        while (visible_messages + delayed_messages + invisible_messages) > 0 and iterations < max_iterations:
            iterations += 1
            print(f"Iteration {iterations} - Visible: {visible_messages}, Invisible: {invisible_messages}, Delayed: {delayed_messages}")
            
            # waiting if no visible messages found
            if visible_messages == 0:
                print("No visible messages. Waiting 30 seconds...")
                time.sleep(30)
            # getting messages
            else:
                try:
                    receive_response = sqs.receive_message(
                        QueueUrl=queue_url,
                        MessageAttributeNames=['All'],
                        MaxNumberOfMessages=1,
                        WaitTimeSeconds=5
                    )
                    # getting message, receipt handle for deletion
                    if 'Messages' in receive_response and len(receive_response['Messages']) > 0:
                        message = receive_response['Messages'][0]
                        receipt_handle = message['ReceiptHandle']
                        
                        # Extract message attributes
                        attrs = message.get('MessageAttributes', {})
                        order_num = int(attrs['order_no']['StringValue'])
                        word = attrs['word']['StringValue']
                        
                        # adding word and order num to dict
                        word_dict[order_num] = word
                        print(f"Received message {order_num}: {word}")
                        
                        # deleting message
                        sqs.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=receipt_handle
                        )
                    else:
                        print("No messages received, waiting...")
                        time.sleep(10)
                        
                except Exception as e:
                    print(f"Error receiving/processing message: {e}")
                    time.sleep(10)
            
            # Refresh queue attributes
            try:
                attr_response = sqs.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=['All']
                )
                
                if "Attributes" in attr_response:
                    visible_messages = int(attr_response["Attributes"]["ApproximateNumberOfMessages"])
                    invisible_messages = int(attr_response["Attributes"]["ApproximateNumberOfMessagesNotVisible"])
                    delayed_messages = int(attr_response["Attributes"]["ApproximateNumberOfMessagesDelayed"])
            except Exception as e:
                print(f"Error refreshing queue attributes: {e}")
                break
        
        print(f"Collection complete. Total messages collected: {len(word_dict)}")
        return word_dict

    @task
      # sort messages and create phrase
    def sort_messages(word_dict):
        if not word_dict:
            raise ValueError("No messages collected - word_dict is empty")
        
        # sorting by integer keys
        int_dict = {int(k): v for k, v in word_dict.items()}
        sorted_items = sorted(int_dict.items())
        phrase = " ".join([word for order_num, word in sorted_items])
        
        print(f"Sorted dictionary: {dict(sorted_items)}")
        print(f"Complete phrase: {phrase}")
        
        # returning completed phrase
        return phrase
    
    @task
    # sending completed solution to queue
    def send_solution(uvaid, phrase, platform_name):
        print(f"Sending solution - UVA ID: {uvaid}, Platform: {platform_name}")
        print(f"Phrase: {phrase}")
        
        # Initialize SQS client
        sqs = boto3.client('sqs', region_name='us-east-1')
        submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
        
        try:
            response = sqs.send_message(
                QueueUrl=submit_url,
                MessageBody=phrase,
                MessageAttributes={
                    'uvaid': {
                        'DataType': 'String',
                        'StringValue': uvaid
                    },
                    'phrase': {
                        'DataType': 'String',
                        'StringValue': phrase
                    },
                    'platform': {
                        'DataType': 'String',
                        'StringValue': platform_name
                    }
                }
            )
            
            print(f"Submission successful! MessageId: {response['MessageId']}")
            return response
            
        except Exception as e:
            print(f"Error sending solution: {e}")
            raise

    # Define task dependencies
    payload = api_request()
    word_dict = collect_messages(payload)
    phrase = sort_messages(word_dict)
    response = send_solution(uva_id, phrase, platform)

taskflow_dag()
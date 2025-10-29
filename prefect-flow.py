from prefect import flow, task
import boto3
import time
import requests

# Defining variables, establishing connection
url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/tfu5hw"
sqs = boto3.client('sqs')
uva_id = "tfu5hw"
platform = "prefect"

# Creating output.txt file to store messages and indices
with open("output.txt", "w") as file:
    file.write("The messages and numbers will be stored below:\n")

# Makes API request to return payload dict
@task(log_prints=True)
def api_request():
    # getting payload dict
    payload = requests.post(url).json()
    # Print/log and return payload
    print(f"payload: {payload}")
    return payload

# Collects messages in queue, returns dictionary of words and indices    
@task(log_prints = True)
def collect_messages(payload):
    # Creating dictionary to store words
    word_dict = {} 
    
    # Getting all attributes with sqs method
    attr_response = sqs.get_queue_attributes(
        QueueUrl=payload["sqs_url"],
        AttributeNames=['All']
    )
    
    # Checking if attributes were collected
    try:
        # Converting message counts to int for loop
        visible_messages = int(attr_response["Attributes"]["ApproximateNumberOfMessages"])
        invisible_messages = int(attr_response["Attributes"]["ApproximateNumberOfMessagesNotVisible"])
        delayed_messages = int(attr_response["Attributes"]["ApproximateNumberOfMessagesDelayed"])
        print(f"Initial counts - Visible: {visible_messages}, Invisible: {invisible_messages}, Delayed: {delayed_messages}")
    
    except Exception as e:
        print(f"Error receiving attributes: {e}")

    # loop executes while there are still messages to be collected
    while((visible_messages + delayed_messages + invisible_messages) != 0):
        # updating counts for logger
        print(f"Current counts - Visible: {visible_messages}, Invisible: {invisible_messages}, Delayed: {delayed_messages}")
        
        # if no messages are visible, wait 30 seconds and try again
        if visible_messages == 0:
            print("No visible messages. Waiting 30 seconds...")
            time.sleep(30)
            # Refresh attributes, get new counts
            attr_response = sqs.get_queue_attributes(
                QueueUrl=payload["sqs_url"],
                AttributeNames=['All']
            )
            if ("Attributes" in attr_response):
                visible_messages = int(attr_response["Attributes"]["ApproximateNumberOfMessages"])
                invisible_messages = int(attr_response["Attributes"]["ApproximateNumberOfMessagesNotVisible"])
                delayed_messages = int(attr_response["Attributes"]["ApproximateNumberOfMessagesDelayed"])
        else:
            try:
                receive_response = sqs.receive_message(
                    QueueUrl=payload['sqs_url'],
                    MessageAttributeNames=['All']
                )
                # checks if messages attribute is callable, length of messages as positive int
                if 'Messages' in receive_response and len(receive_response['Messages']) > 0:
                    # get handle for message deletion
                    receipt_handle = receive_response['Messages'][0]['ReceiptHandle']
                    # Save number of message ordering number
                    order_num = int(receive_response['Messages'][0]['MessageAttributes']['order_no']['StringValue'])
                    # Save word
                    word = receive_response['Messages'][0]['MessageAttributes']['word']['StringValue']
                    # Appending word to dictionary with num key
                    word_dict[order_num] = word
                    print(f"Received message {order_num}: {word}")
                    # Writing the word and message number to text file
                    with open("output.txt", "a") as file:
                        file.write(f"{order_num}, {word}\n")
                    # Supposed to delete message(make sure it is doing so)
                    del_response = sqs.delete_message(QueueUrl=payload["sqs_url"], ReceiptHandle=receipt_handle)
                    if del_response['ResponseMetadata']['HTTPStatusCode'] == 200:
                        print(f"Successfully deleted message {order_num}")
                    else:
                        print(f"Failed to delete message {order_num}")
                else:
                    print("No messages in response, waiting...")
                    time.sleep(15)
            except Exception as e:
                print(f"Error receiving message: {e}")
                time.sleep(15)
                
            # Refresh attributes
            attr_response = sqs.get_queue_attributes(
                QueueUrl=payload["sqs_url"],
                AttributeNames=['All']
            )
            
            if ("Attributes" in attr_response):
                visible_messages = int(attr_response["Attributes"]["ApproximateNumberOfMessages"])
                invisible_messages = int(attr_response["Attributes"]["ApproximateNumberOfMessagesNotVisible"])
                delayed_messages = int(attr_response["Attributes"]["ApproximateNumberOfMessagesDelayed"])
    
    #Returning word dict to be passed to next task
    print("All messages collected")
    return word_dict 

@task(log_prints=True)
# sorts collected messages into comprehensive phrase
def sort_messages(word_dict, log_prints = True):
    # sort by key into correct order
    sorted_items = sorted(word_dict.items())
    # join method to make comprehensive phrase from all words
    phrase = " ".join([word for order_num, word in sorted_items])
    # prints the sorted dict and phrase
    print(f"Sorted dictionary: {dict(sorted_items)}")
    print(f"Complete phrase: {phrase}")
    # return phrase to be sent as solution
    return phrase

@task(log_prints = True)
# sends solution
def send_solution(uvaid, phrase, platform):
    submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    # sending all necessary components in attributes
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
                    'StringValue': platform
                }
            }
        )
        # Checks if message was correctly sent
        print(f"Response: {response}")
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print("Message sent successfully")
        else:
            print("Message not sent respectfully")

    except Exception as e:
            print(f"Error sending solution: {e}")
            raise e
    return(response)


# compiles all tasks into comprehensive workflow
@flow
def my_flow():
    payload_var = api_request()
    word_dict = collect_messages(payload_var) 
    phrase = sort_messages(word_dict)  
    response = send_solution(uva_id, phrase, platform)
    return phrase, response

if __name__ == "__main__":
    my_flow()
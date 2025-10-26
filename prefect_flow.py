from prefect import flow, task
import boto3
import time
import requests

url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/tfu5hw"
sqs = boto3.client('sqs')
uva_id = "tfu5hw"
platform = "prefect"

with open("output.txt", "w") as file:
    file.write("The messages and numbers will be stored below:\n")


@task(log_prints=True)
def api_request(log_prints = True):
    # getting payload dict
    payload = requests.post(url).json()
    print(f"payload: {payload}")
    return payload
    
@task(log_prints = True)
def collect_messages(payload):
    word_dict = {} 
    
    attr_response = sqs.get_queue_attributes(
        QueueUrl=payload["sqs_url"],
        AttributeNames=['All']
    )
    
    if ("Attributes" in attr_response):
        visible_messages = int(attr_response["Attributes"]["ApproximateNumberOfMessages"])
        invisible_messages = int(attr_response["Attributes"]["ApproximateNumberOfMessagesNotVisible"])
        delayed_messages = int(attr_response["Attributes"]["ApproximateNumberOfMessagesDelayed"])
        print(f"Initial counts - Visible: {visible_messages}, Invisible: {invisible_messages}, Delayed: {delayed_messages}")
    else:
        print("Attributes field not found")
        return word_dict

    while((visible_messages + delayed_messages + invisible_messages) != 0):
        print(f"Current counts - Visible: {visible_messages}, Invisible: {invisible_messages}, Delayed: {delayed_messages}")
        
        if visible_messages == 0:
            print("No visible messages. Waiting 30 seconds...")
            time.sleep(30)
            # Refresh attributes
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
                if 'Messages' in receive_response and len(receive_response['Messages']) > 0:
                    receipt_handle = receive_response['Messages'][0]['ReceiptHandle']
                    order_num = int(receive_response['Messages'][0]['MessageAttributes']['order_no']['StringValue'])
                    word = receive_response['Messages'][0]['MessageAttributes']['word']['StringValue']
                    word_dict[order_num] = word
                    print(f"Received message {order_num}: {word}")
                    with open("output.txt", "a") as file:
                        file.write(f"{order_num}, {word}\n")
                    del_response = sqs.delete_message(QueueUrl=payload["sqs_url"], ReceiptHandle=receipt_handle)
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
    
    print("All messages collected")
    return word_dict 



@task(log_prints=True)
def sort_messages(word_dict, log_prints = True):
    sorted_items = sorted(word_dict.items())
    phrase = " ".join([word for order_num, word in sorted_items])
    print(f"Sorted dictionary: {dict(sorted_items)}")
    print(f"Complete phrase: {phrase}")
    return phrase

@task(log_prints = True)
def send_solution(uvaid, phrase, platform):
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
                    'StringValue': platform
                }
            }
        )
        print(f"Response: {response}")
    except Exception as e:
            print(f"Error sending solution: {e}")
            raise e
    return(response)



@flow
def my_flow():
    payload_var = api_request()
    word_dict = collect_messages(payload_var) 
    phrase = sort_messages(word_dict)  
    response = send_solution(uva_id, phrase, platform)
    return phrase, response

if __name__ == "__main__":
    my_flow()
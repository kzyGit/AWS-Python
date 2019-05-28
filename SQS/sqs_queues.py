from botocore.exceptions import ClientError
import logging
import boto3
sqs = boto3.client('sqs')


def all_queues():
    response = sqs.list_queues()
    print("\nAvailable queues\n\n", response['QueueUrls'], "\n")


def create_queue(name):
    try:
        response = sqs.create_queue(QueueName=name,
                                    Attributes={
                                        'DelaySeconds': '60',
                                        'MessageRetentionPeriod': '86400'
                                    })
        responce = "Queue created with url: " + response['QueueUrl']
    except Exception:
        responce = 'None created, queue with similar name already exists'
    print(responce)


def delete_queue(name):
    try:
        sqs.delete_queue(QueueUrl=name)
        res = 'Queue ' + name + ' deleted successfully'
    except Exception:
        res = 'Sorry, queue  ' + name + ' does not exist, queue, not deleted'
    print(res)


def send_message(sqs_queue_url):
    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s: %(asctime)s: %(message)s')

    # Send some SQS messages
    for i in range(1, 6):
        msg_body = f'SQS message #{i}'
        try:
            msg = sqs.send_message(QueueUrl=sqs_queue_url,
                                   MessageBody=msg_body)
        except ClientError as e:
            logging.error(e)
            return None
    if msg is not None:
        logging.info(f'Sent SQS message ID: {msg["MessageId"]}')


def receive_message(sqs_queue_url, num_msgs=2, wait_time=0, visibility_time=5):
    if num_msgs < 1:
        num_msgs = 1
    elif num_msgs > 10:
        num_msgs = 10
    try:
        msgs = sqs.receive_message(QueueUrl=sqs_queue_url,
                                   MaxNumberOfMessages=num_msgs,
                                   WaitTimeSeconds=wait_time,
                                   VisibilityTimeout=visibility_time)
    except ClientError:
        print("****** None")

    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s: %(asctime)s: %(message)s')

    # Retrieve SQS messages
    try:
        msgs = msgs['Messages']
        if msgs is not None:
            for msg in msgs:
                logging.info(f'SQS: Message ID: {msg["MessageId"]}, '
                             f'Contents: {msg["Body"]}')

                # Remove the message from the queue
                delete_sqs_message(sqs_queue_url, msg['ReceiptHandle'])

    except Exception:
        print("No messages available in this queue")


def delete_sqs_message(sqs_queue_url, msg_receipt_handle):
    sqs.delete_message(QueueUrl=sqs_queue_url,
                       ReceiptHandle=msg_receipt_handle)

from confluent_kafka import Producer


def print_log_message(error, message):
    if error is not None:
        print('Error: {}'.format(error))
    else:
        print('Produced message with value: {}\n'.format(message.value().decode('utf-8')))


# Set Kafka Broker
broker = 'localhost:9092'

# Set Kafka topic
topic = 'log_topic1'

# Producer configurations
producer = Producer({
    'bootstrap.servers': broker,
    'api.version.request': True
})
print('Kafka Producer started')

index = 1
with open('42MBSmallServerLog.log', 'r') as file:
    batch_file_array = []
    for line in file:
        # Add each line to the current batch
        batch_file_array.append(line)

        # If batch array is full reset it
        # Set batch size to 10000
        if len(batch_file_array) >= 50:
            producer.produce(topic, key=str(index), value=''.join(batch_file_array).encode('utf-8'))
            producer.flush()
            batch_file_array = []
        index += 1

    # Remainder lines
    if batch_file_array:
        producer.poll(1)
        producer.produce(topic, key=str(index), value=''.join(batch_file_array).encode('utf-8'), callback=print_log_message)
        producer.flush()

producer.flush()
producer.produce(topic, key="DONE", value="FINISHED", callback=print_log_message)
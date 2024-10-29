import pika
import json
from dotenv import dotenv_values

config = dotenv_values()

# Настройки для RabbitMQ
rabbitmq_host = config.get('RABBIT_HOST')
queue_name = config.get('QUEUE_NAME')
rabbit_user = config.get('RABBIT_USER')
rabbit_password = config.get('RABBIT_PASSWORD')


# Функция обработки сообщения
def process_message(channel, method, properties, body):
    message = json.loads(body)
    print("Received message:", message)

    # Подтверждаем успешную обработку сообщения
    channel.basic_ack(delivery_tag=method.delivery_tag)


# Создание подключения к RabbitMQ
credentials = pika.PlainCredentials(rabbit_user, rabbit_password)
connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
channel = connection.channel()

# Подписываемся на очередь
channel.queue_declare(queue=queue_name, durable=True)

# Подключаем consumer
channel.basic_consume(queue=queue_name, on_message_callback=process_message)

print(f"Waiting for messages in {queue_name}. To exit press CTRL+C")
channel.start_consuming()

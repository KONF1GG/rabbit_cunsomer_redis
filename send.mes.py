import pika
import json
from dotenv import dotenv_values

config = dotenv_values()

# Настройки для RabbitMQ
rabbitmq_host = config.get('RABBIT_HOST')
queue_name = config.get('QUEUE_NAME')
rabbit_user = config.get('RABBIT_USER')
rabbit_password = config.get('RABBIT_PASSWORD')

# Создание подключения к RabbitMQ
credentials = pika.PlainCredentials(rabbit_user, rabbit_password)
connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
channel = connection.channel()

# Объявляем очередь (если она еще не существует)
channel.queue_declare(queue=queue_name, durable=True)

# Создаем сообщение
message = {
    "key": "login:test",
    "value": {
        "field1": "value3",
        "field2": "value2"
    },
    "createIfNot": True,
}

# Преобразуем сообщение в JSON
message_json = json.dumps(message)

# Отправляем сообщение в очередь
channel.basic_publish(
    exchange='',               # Используем стандартный обменник
    routing_key=queue_name,   # Имя очереди
    body=message_json,        # Сообщение
    properties=pika.BasicProperties(
        delivery_mode=2,      # Делает сообщение долговечным
    )
)

print(f"Message sent to queue '{queue_name}': {message_json}")

# Закрываем соединение
connection.close()

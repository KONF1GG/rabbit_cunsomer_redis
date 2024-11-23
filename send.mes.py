import pika
import json
from dotenv import dotenv_values

config = dotenv_values()

# Настройки для RabbitMQ
rabbitmq_host = config.get('RABBIT_HOST')
queue_name = config.get('QUEUE_NAME')
exchange_name = "to_redis"  # Название обменника
rabbit_user = config.get('RABBIT_USER')
rabbit_password = config.get('RABBIT_PASSWORD')

# Создание подключения к RabbitMQ
credentials = pika.PlainCredentials(rabbit_user, rabbit_password)
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials)
)
channel = connection.channel()

# Объявляем обменник
channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)

# Объявляем очередь и связываем её с обменником
channel.queue_declare(queue=queue_name, durable=True, arguments={"x-message-ttl": 86400000})  # TTL на 24 часа
channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=queue_name)

# Создаем сообщение
message = {
    "key": "phone:9999999999999",
    "value": {
        "field1": "value3",
        "field2": "value2"
    },
    "createIfNot": True,
}

# Преобразуем сообщение в JSON
message_json = json.dumps(message)

# Отправляем сообщение в очередь через обменник
channel.basic_publish(
    exchange=exchange_name,        # Указываем обменник
    routing_key=queue_name,        # Имя очереди в качестве ключа маршрутизации
    body=message_json,             # Сообщение
    properties=pika.BasicProperties(
        delivery_mode=2,           # Делает сообщение долговечным
    )
)

print(f"Message sent to exchange '{exchange_name}' with routing key '{queue_name}': {message_json}")

# Закрываем соединение
connection.close()

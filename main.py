import pika
import redis
import json
from dotenv import dotenv_values

config = dotenv_values()

# Настройки для RabbitMQ
rabbitmq_host = config.get('RABBIT_HOST')
queue_name = config.get('QUEUE_NAME')
rabbit_user = config.get('RABBIT_USER')
rabbit_password = config.get('RABBIT_PASSWORD')

# Настройки для Redis
redis_host = config.get('REDIS_HOST')
redis_port = config.get('REDIS_PORT')
redis_username = config.get('REDIS_USERNAME')
redis_password = config.get('REDIS_PASSWORD')

# Подключаемся к Redis
redis_client = redis.StrictRedis(
    host=redis_host,
    port=redis_port,
    username=redis_username,
    password=redis_password,
    decode_responses=True
)


def process_message(channel, method, properties, body):

    message = json.loads(body)
    print(message)
    key = message.get("key")
    value = message.get("value")
    create_if_not = message.get("createIfNot", False)
    ttl = message.get("ttl", None)

    # Проверяем, существует ли ключ в Redis
    if not redis_client.exists(key):

        if create_if_not:
            # Записываем целиком value в Redis
            redis_client.set(key, json.dumps(value))
            if ttl:
                redis_client.expire(key, ttl)
        # Если createIfNot=False, ничего не делаем, просто удаляем сообщение из очереди
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    current_value = redis_client.get(key)
    current_value = json.loads(current_value)
    current_value.update(value)
    redis_client.set(key, json.dumps(current_value))

    # Устанавливаем TTL, если передан
    if ttl:
        redis_client.expire(key, ttl)

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

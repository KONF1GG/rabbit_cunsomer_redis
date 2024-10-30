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

# Подключаемся к Redis с поддержкой RedisJSON
redis_client = redis.StrictRedis(
    host=redis_host,
    port=redis_port,
    username=redis_username,
    password=redis_password,
    decode_responses=True
)


def process_message(channel, method, properties, body):
    try:
        message = json.loads(body)
        key = message.get("key")
        value = message.get("value")
        create_if_not = message.get("createIfNot", False)
        ttl = message.get("ttl", None)

        # Проверка существования ключа в Redis
        if not redis_client.exists(key):
            if create_if_not:
                redis_client.json().set(key, '.', value)
                if ttl:
                    redis_client.expire(key, ttl)
        else:
            # Если ключ существует, обновляем текущие данные
            current_value = redis_client.json().get(key)
            current_value.update(value)
            redis_client.json().set(key, '.', current_value)
            if ttl:
                redis_client.expire(key, ttl)

        # Подтверждаем успешную обработку сообщения
        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Error processing message: {e}")
        # channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


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

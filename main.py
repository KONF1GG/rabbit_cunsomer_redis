import datetime
import pika
import redis
import clickhouse_connect
import json
from dotenv import dotenv_values

config = dotenv_values('.env')

# Настройки RabbitMQ
RABBIT_HOST = config.get('RABBIT_HOST')
RABBIT_USER = config.get('RABBIT_USER')
RABBIT_PASSWORD = config.get('RABBIT_PASSWORD')
QUEUE_NAME = config.get('QUEUE_NAME')
ERROR_QUEUE_NAME = 'error_queue'

# Настройки Redis
REDIS_HOST = config.get('REDIS_HOST')
REDIS_PORT = config.get('REDIS_PORT')
REDIS_PASSWORD = config.get('REDIS_PASSWORD')

# Настройки ClickHouse
CLICKHOUSE_HOST = config.get('HOST')
CLICKHOUSE_PORT = config.get('PORT')
CLICKHOUSE_DATABASE = config.get('DATABASE')
CLICKHOUSE_USER = config.get('USER')
CLICKHOUSE_PASSWORD = config.get('PASSWORD')

# Подключение к Redis с поддержкой RedisJSON
redis_client = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True
)


def log_to_clickhouse(client, key, message_data, status='success', error=None):
    query = f"""
    INSERT INTO rabbitmq.logs (key, payload, status, error, timestamp)
    VALUES ('{key}', '{json.dumps(message_data)}', '{status}', '{error}', '{datetime.datetime.now()}')
    """
    client.command(query)


def process_message(ch, method, properties, body, redis_client, clickhouse_client):
    message_data = json.loads(body)
    key = message_data.get('key')
    create_if_not = message_data.get('createIfNot', True)

    try:
        value = message_data.get('value', {})

        if redis_client.exists(key):
            current_value = redis_client.json().get(key)
            current_value.update(value)
            redis_client.json().set(key, '.', current_value)
            status = 'updated'
        else:
            if create_if_not:
                redis_client.json().set(key, '.', value)
                status = 'inserted'
            else:
                status = 'skipped'

        log_to_clickhouse(clickhouse_client, key, message_data, status=status)

    except Exception as e:
        print(f"Error processing message: {e}")
        log_to_clickhouse(clickhouse_client, key, message_data, status='error', error=str(e))

        # Перенаправление сообщения в другую очередь
        ch.basic_publish(
            exchange='',
            routing_key=ERROR_QUEUE_NAME,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,
            )
        )


def main():
    # Подключение к ClickHouse
    clickhouse_client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )

    # Подключение к RabbitMQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBIT_HOST,
            credentials=pika.PlainCredentials(RABBIT_USER, RABBIT_PASSWORD)
        )
    )

    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_declare(queue=ERROR_QUEUE_NAME, durable=True)

    def callback(ch, method, properties, body):
        process_message(ch, method, properties, body, redis_client, clickhouse_client)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)

    print("Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()


if __name__ == '__main__':
    main()

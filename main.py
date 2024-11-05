import datetime
import pika
import redis
import clickhouse_connect
import json
import time
from threading import Thread
from dotenv import dotenv_values

config = dotenv_values('.env')

# Настройки RabbitMQ
RABBIT_HOST = config.get('RABBIT_HOST')
RABBIT_USER = config.get('RABBIT_USER')
RABBIT_PASSWORD = config.get('RABBIT_PASSWORD')
QUEUE_NAME = config.get('QUEUE_NAME')

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

redis_client = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True
)

last_message_time = time.time()


def log_to_clickhouse(client, key, message_data, status='success', error=None):
    key_str = str(key) if key else ''
    message_data_str = json.dumps(message_data).replace("'", "''")
    status_str = str(status) if status else 'unknown'
    error_str = str(error).replace("'", "''") if error else ''
    timestamp = datetime.datetime.now().isoformat()

    query = f"""
    INSERT INTO rabbitmq.logs (key, payload, status, error, timestamp)
    VALUES ('{key_str}', '{message_data_str}', '{status_str}', '{error_str}', '{timestamp}')
    """
    client.command(query)


def process_message(ch, method, properties, body, redis_client, clickhouse_client):
    global last_message_time
    last_message_time = time.time()  # обновляем время последнего сообщения
    message_data = json.loads(body)
    key = message_data.get('key')
    create_if_not = message_data.get('createIfNot', True)
    ttl = message_data.get('ttl', None)

    try:
        value = message_data.get('value', {})
        status = None

        if redis_client.exists(key):
            # Пытаемся обновить существующее значение
            current_value = redis_client.json().get(key)
            current_value.update(value)
            success = redis_client.json().set(key, '.', current_value)
            if ttl and success:
                redis_client.expire(key, ttl)
            status = 'updated' if success else 'failed_to_update'
        else:
            if create_if_not:
                # Пытаемся вставить новое значение
                success = redis_client.json().set(key, '.', value)
                if ttl and success:
                    redis_client.expire(key, ttl)
                status = 'inserted' if success else 'failed_to_insert'
            else:
                status = 'skipped'

        if status in {'inserted', 'updated'}:
            log_to_clickhouse(clickhouse_client, key, message_data, status=status)
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Подтверждаем только если обновление прошло успешно
        else:
            # Логируем ошибку, если вставка или обновление не прошли
            error_message = f"Failed to {'inserted' if status == 'failed_to_insert' else 'updated'} message in Redis."
            log_to_clickhouse(clickhouse_client, key, message_data, status='error', error=error_message)

    except Exception as e:
        print(f"Error processing message: {e}")
        log_to_clickhouse(clickhouse_client, key, message_data, status='error', error=str(e))
        # Сообщение не подтверждается и останется в очереди для повторной обработки


def setup_rabbitmq_channel():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBIT_HOST,
                    credentials=pika.PlainCredentials(RABBIT_USER, RABBIT_PASSWORD)
                )
            )
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            return connection, channel
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Connection error: {e}. Retrying in 5 seconds...")
            time.sleep(5)


def monitor_connection(channel):
    """Функция для перезапуска подключения, если сообщений нет более минуты."""
    global last_message_time
    while True:
        if time.time() - last_message_time > 60:
            print("No messages received in the last minute. Reconnecting...")
            if channel.is_open:
                channel.stop_consuming()
            break
        time.sleep(10)


def main():
    global last_message_time
    clickhouse_client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )

    while True:
        connection, channel = setup_rabbitmq_channel()

        # Обработчик основной очереди
        def callback(ch, method, properties, body):
            process_message(ch, method, properties, body, redis_client, clickhouse_client)

        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=False)

        # Запускаем мониторинг активности сообщений
        monitor_thread = Thread(target=monitor_connection, args=(channel,))
        monitor_thread.start()

        print("Waiting for messages. To exit press CTRL+C")
        try:
            channel.start_consuming()
            last_message_time = time.time()
        except pika.exceptions.ConnectionClosed:
            print("Connection lost. Attempting to reconnect...")
            continue
        except pika.exceptions.StreamLostError as e:
            print(f"Stream lost: {e}. Attempting to reconnect...")
            continue
        finally:
            if connection.is_open:
                connection.close()


if __name__ == '__main__':
    main()

import datetime
import pika
import redis
import clickhouse_connect
import json
import time
from threading import Thread
import pytz
import requests
from dotenv import dotenv_values
import psycopg2
from psycopg2.extras import RealDictCursor

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

# Настройки PostgreSQL
POSTGRES_HOST = config.get('RBT_HOST')
POSTGRES_PORT = config.get('RBT_PORT')
POSTGRES_USER = config.get('RBT_USER')
POSTGRES_PASSWORD = config.get('RBT_PASSWORD')
POSTGRES_DATABASE = config.get('RBT_DATABASE')

redis_client = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True
)

API_TOKEN = config.get('API_TOKEN')
CHAT_ID = config.get('CHAT_ID')

last_message_time = time.time()


def send_telegram_message(message):
    url = f'https://api.telegram.org/bot{API_TOKEN}/sendMessage'
    data = {'chat_id': CHAT_ID, 'text': message}
    try:
        response = requests.post(url, data=data)
        if response.status_code != 200:
            print(f"Ошибка отправки уведомления: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"Ошибка при отправке уведомления: {e}")


def log_to_clickhouse(client, key, message_data, status='success', error=None):
    key_str = str(key) if key else ''
    message_data_str = json.dumps(message_data).replace("'", "''")
    status_str = str(status) if status else 'unknown'
    error_str = str(error).replace("'", "''") if error else ''

    # Устанавливаем часовой пояс GMT+5
    timezone = pytz.timezone('Etc/GMT-5')
    timestamp = datetime.datetime.now(timezone).strftime('%Y-%m-%d %H:%M:%S')

    query = f"""
    INSERT INTO rabbitmq.logs (key, payload, status, error, timestamp)
    VALUES ('{key_str}', '{message_data_str}', '{status_str}', '{error_str}', '{timestamp}')
    """
    client.command(query)


def check_rbt_status(phone):
    """
    Проверяет статус RBT для заданного номера телефона в базе данных.
    Возвращает True, если запись найдена и актуальна, иначе False.
    """
    connection = None
    try:
        connection = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DATABASE,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            sslmode='disable'
        )
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            query = f"SELECT last_seen, auth_token, house_subscriber_id FROM houses_subscribers_mobile WHERE id = '7{phone}';"
            cursor.execute(query)
            result = cursor.fetchone()

            if result:
                last_seen = result['last_seen']

                last_seen = datetime.datetime.fromtimestamp(last_seen)

                # Сравнение с текущей датой минус 90 дней
                three_months_ago = datetime.datetime.now() - datetime.timedelta(days=90)
                if last_seen > three_months_ago:
                    return result
                else:
                    return None

    except Exception as e:
        send_telegram_message(e)
        print(f"Error querying RBT database: {e}")
        return False
    finally:
        if connection:
            connection.close()


def process_message(ch, method, properties, body, redis_client, clickhouse_client):
    global last_message_time
    last_message_time = time.time()
    message_data = json.loads(body)

    key = message_data.get('key')
    create_if_not = message_data.get('createIfNot', True)
    ttl = message_data.get('ttl', None)
    replace = message_data.get('replace', False)

    try:
        value = message_data.get('value', {})
        status = None

        # Проверяем ключ на соответствие phone:...
        if key.startswith('phone:'):
            phone = key.split(':', 1)[1]
            rbt_result = check_rbt_status(phone)
            if rbt_result:
                value['rbt'] = True
                value['auth_token'] = rbt_result['auth_token']
                value['house_subscriber_id'] = rbt_result['house_subscriber_id']
            else:
                value['rbt'] = False
        if redis_client.exists(key):
            if replace:
                # Полностью заменить данные
                success = redis_client.json().set(key, '.', value)
                if ttl and success:
                    redis_client.expire(key, ttl)
                status = 'replaced' if success else 'failed_to_replace'
            else:
                # Обновить существующие данные
                current_value = redis_client.json().get(key)
                current_value.update(value)
                success = redis_client.json().set(key, '.', current_value)
                if ttl and success:
                    redis_client.expire(key, ttl)
                status = 'updated' if success else 'failed_to_update'
        else:
            if create_if_not:
                success = redis_client.json().set(key, '.', value)
                if ttl and success:
                    redis_client.expire(key, ttl)
                status = 'inserted' if success else 'failed_to_insert'
            else:
                status = 'skipped'

        if status in {'inserted', 'updated', 'replaced'}:
            log_to_clickhouse(clickhouse_client, key, message_data, status=status)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            error_message = f"Failed to {'insert' if status == 'failed_to_insert' else 'update' if status == 'failed_to_update' else 'replace' if status == 'failed_to_replace' else 'to process'} message in Redis."
            log_to_clickhouse(clickhouse_client, key, message_data, status='error', error=error_message)
            send_telegram_message(error_message)

    except Exception as e:
        print(f"Error processing message: {e}")
        send_telegram_message(e)
        log_to_clickhouse(clickhouse_client, key, message_data, status='error', error=str(e))


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
            channel.queue_declare(
                queue=QUEUE_NAME,
                durable=True,
                arguments={'x-message-ttl': 86400000}
            )
            return connection, channel
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Connection error: {e}. Retrying in 5 seconds...")
            time.sleep(5)


def monitor_connection(channel):
    global last_message_time
    while True:
        if time.time() - last_message_time > 600:
            print("No messages received in the last 10 minutes. Reconnecting...")
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

        def callback(ch, method, properties, body):
            try:
                process_message(ch, method, properties, body, redis_client, clickhouse_client)
            except Exception as e:
                print(f"Callback error: {e}. Re-queueing message.")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=False)

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
    # print(check_rbt_status('99999999999999'))

import datetime
import pika
from pika.exceptions import ConnectionClosed, StreamLostError, AMQPConnectionError
import redis
import clickhouse_connect
import json
import time
import logging
import sys
from threading import Thread
from typing import Optional, Dict, Any
import pytz
import requests
from dotenv import dotenv_values
import psycopg2
from psycopg2.extras import RealDictCursor


# Настройка логирования
def setup_logging():
    """Настройка системы логирования"""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("consumer.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger(__name__)


logger = setup_logging()


class Config:
    """Класс для управления конфигурацией"""

    def __init__(self):
        config = dotenv_values(".env")

        # Настройки RabbitMQ
        self.rabbit_host = self._get_required_config(config, "RABBIT_HOST")
        self.rabbit_user = self._get_required_config(config, "RABBIT_USER")
        self.rabbit_password = self._get_required_config(config, "RABBIT_PASSWORD")
        self.queue_name = self._get_required_config(config, "QUEUE_NAME")

        # Настройки Redis
        self.redis_host = self._get_required_config(config, "REDIS_HOST")
        self.redis_port = int(self._get_required_config(config, "REDIS_PORT"))
        self.redis_password = config.get("REDIS_PASSWORD")

        # Настройки ClickHouse
        self.clickhouse_host = self._get_required_config(config, "HOST")
        self.clickhouse_port = config.get("PORT")
        self.clickhouse_database = config.get("DATABASE")
        self.clickhouse_user = self._get_required_config(config, "USER")
        self.clickhouse_password = self._get_required_config(config, "PASSWORD")

        # Настройки PostgreSQL
        self.postgres_host = self._get_required_config(config, "RBT_HOST")
        self.postgres_port = int(self._get_required_config(config, "RBT_PORT"))
        self.postgres_user = self._get_required_config(config, "RBT_USER")
        self.postgres_password = self._get_required_config(config, "RBT_PASSWORD")
        self.postgres_database = self._get_required_config(config, "RBT_DATABASE")

        # Настройки Telegram
        self.api_token = self._get_required_config(config, "API_TOKEN")
        self.chat_id = self._get_required_config(config, "CHAT_ID")

    def _get_required_config(self, config: Dict[str, Optional[str]], key: str) -> str:
        """Получает обязательный параметр конфигурации"""
        value = config.get(key)
        if not value:
            raise ValueError(
                f"Required configuration parameter '{key}' is missing or empty"
            )
        return value


app_config = Config()

redis_client = redis.StrictRedis(
    host=app_config.redis_host,
    port=app_config.redis_port,
    password=app_config.redis_password,
    decode_responses=True,
)

last_message_time = time.time()


def send_telegram_message(message: str) -> None:
    """Отправка уведомления в Telegram"""
    url = f"https://api.telegram.org/bot{app_config.api_token}/sendMessage"
    data = {"chat_id": app_config.chat_id, "text": message}
    try:
        response = requests.post(url, data=data, timeout=10)
        if response.status_code != 200:
            logger.error(
                f"Ошибка отправки уведомления в Telegram: {response.status_code}, {response.text}"
            )
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления в Telegram: {e}")


def log_to_clickhouse(
    client,
    key: str,
    message_data: Dict[str, Any],
    status: str = "success",
    error: Optional[str] = None,
) -> None:
    """Логирование событий в ClickHouse"""
    try:
        key_str = str(key) if key else ""
        message_data_str = json.dumps(message_data).replace("'", "''")
        status_str = str(status) if status else "unknown"
        error_str = str(error).replace("'", "''") if error else ""

        # Устанавливаем часовой пояс GMT+5
        timezone = pytz.timezone("Etc/GMT-5")
        timestamp = datetime.datetime.now(timezone).strftime("%Y-%m-%d %H:%M:%S")

        query = f"""
        INSERT INTO rabbitmq.logs (key, payload, status, error, timestamp)
        VALUES ('{key_str}', '{message_data_str}', '{status_str}', '{error_str}', '{timestamp}')
        """
        client.command(query)
        logger.debug(f"Logged to ClickHouse: key={key_str}, status={status_str}")
    except Exception as e:
        logger.error(f"Ошибка при записи в ClickHouse: {e}")


def check_rbt_status(phone: str) -> Optional[Dict[str, Any]]:
    """
    Проверяет статус RBT для заданного номера телефона в базе данных.
    Возвращает данные пользователя, если запись найдена и актуальна, иначе None.
    """
    connection = None
    try:
        connection = psycopg2.connect(
            host=app_config.postgres_host,
            port=app_config.postgres_port,
            database=app_config.postgres_database,
            user=app_config.postgres_user,
            password=app_config.postgres_password,
            sslmode="disable",
        )
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            query = "SELECT last_seen, auth_token, house_subscriber_id FROM houses_subscribers_mobile WHERE id = %s"
            cursor.execute(query, (f"7{phone}",))
            result = cursor.fetchone()

            if result:
                last_seen = result["last_seen"]
                last_seen = datetime.datetime.fromtimestamp(last_seen)

                # Сравнение с текущей датой минус 90 дней
                three_months_ago = datetime.datetime.now() - datetime.timedelta(days=90)
                if last_seen > three_months_ago:
                    logger.debug(f"RBT status found for phone {phone}: active")
                    return result
                else:
                    logger.debug(
                        f"RBT status found for phone {phone}: inactive (last_seen: {last_seen})"
                    )
                    return None
            else:
                logger.debug(f"RBT status not found for phone {phone}")
                return None

    except Exception as e:
        error_msg = f"Error querying RBT database for phone {phone}: {e}"
        logger.error(error_msg)
        send_telegram_message(f"redis_consumer: {error_msg}")
        return None
    finally:
        if connection:
            connection.close()


def process_message(
    ch, method, properties, body, redis_client, clickhouse_client
) -> None:
    """Обработка сообщения из RabbitMQ"""
    global last_message_time
    last_message_time = time.time()

    try:
        message_data = json.loads(body)
        logger.info(
            f"Processing message with key: {message_data.get('key', 'unknown')}"
        )
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    key = message_data.get("key")
    create_if_not = message_data.get("createIfNot", True)
    ttl = message_data.get("ttl", None)
    replace = message_data.get("replace", False)

    if not key:
        logger.error("Message missing required 'key' field")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    try:
        value = message_data.get("value", {})
        status = None

        # Проверяем ключ на соответствие phone:...
        if key.startswith("phone:"):
            phone = key.split(":", 1)[1]
            rbt_result = check_rbt_status(phone)
            if rbt_result:
                value["rbt"] = True
                value["auth_token"] = rbt_result["auth_token"]
                value["house_subscriber_id"] = rbt_result["house_subscriber_id"]
                logger.debug(f"Added RBT data for phone {phone}")
            else:
                value["rbt"] = False
                logger.debug(f"No RBT data for phone {phone}")

        if redis_client.exists(key):
            if replace:
                # Полностью заменить данные
                success = redis_client.json().set(key, ".", value)
                if ttl and success:
                    redis_client.expire(key, ttl)
                status = "replaced" if success else "failed_to_replace"
            else:
                # Обновить существующие данные
                current_value = redis_client.json().get(key)
                current_value.update(value)
                success = redis_client.json().set(key, ".", current_value)
                if ttl and success:
                    redis_client.expire(key, ttl)
                status = "updated" if success else "failed_to_update"
        else:
            if create_if_not:
                success = redis_client.json().set(key, ".", value)
                if ttl and success:
                    redis_client.expire(key, ttl)
                status = "inserted" if success else "failed_to_insert"
            else:
                status = "skipped"

        if status in {"inserted", "updated", "replaced"}:
            logger.info(f"Successfully {status} key: {key}")
            log_to_clickhouse(clickhouse_client, key, message_data, status=status)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            error_message = f"Failed to process message for key {key}: {status}"
            logger.info(error_message)
            log_to_clickhouse(
                clickhouse_client,
                key,
                message_data,
                status="error",
                error=error_message,
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    except Exception as e:
        error_message = f"Error processing message for key {key}: {e}"
        logger.error(error_message)
        send_telegram_message(f"redis_consumer: {error_message}")
        log_to_clickhouse(
            clickhouse_client, key, message_data, status="error", error=str(e)
        )
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def setup_rabbitmq_channel():
    """Настройка канала RabbitMQ с переподключением"""
    while True:
        try:
            logger.info("Attempting to connect to RabbitMQ...")
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=app_config.rabbit_host,
                    credentials=pika.PlainCredentials(
                        app_config.rabbit_user, app_config.rabbit_password
                    ),
                )
            )
            channel = connection.channel()
            channel.queue_declare(
                queue=app_config.queue_name,
                durable=True,
                arguments={"x-message-ttl": 86400000},
            )
            logger.info("Successfully connected to RabbitMQ")
            return connection, channel
        except AMQPConnectionError as e:
            logger.error(f"AMQP Connection error: {e}. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected connection error: {e}. Retrying in 5 seconds...")
            time.sleep(5)


def monitor_connection(channel) -> None:
    """Мониторинг подключения и переподключение при необходимости"""
    global last_message_time
    while True:
        if time.time() - last_message_time > 600:
            logger.warning(
                "No messages received in the last 10 minutes. Reconnecting..."
            )
            if channel.is_open:
                channel.stop_consuming()
            break
        time.sleep(10)


def main() -> None:
    """Основная функция приложения"""
    global last_message_time

    logger.info("Starting Redis Consumer application...")

    try:
        clickhouse_client = clickhouse_connect.get_client(
            host=app_config.clickhouse_host,
            username=app_config.clickhouse_user,
            password=app_config.clickhouse_password,
        )
        logger.info("Connected to ClickHouse")
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {e}")
        send_telegram_message(f"redis_consumer: Failed to connect to ClickHouse: {e}")
        return

    while True:
        try:
            connection, channel = setup_rabbitmq_channel()

            def callback(ch, method, properties, body):
                try:
                    process_message(
                        ch, method, properties, body, redis_client, clickhouse_client
                    )
                except Exception as e:
                    logger.error(f"Callback error: {e}. Re-queueing message.")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            channel.basic_consume(
                queue=app_config.queue_name,
                on_message_callback=callback,
                auto_ack=False,
            )

            monitor_thread = Thread(target=monitor_connection, args=(channel,))
            monitor_thread.daemon = True
            monitor_thread.start()

            logger.info("Waiting for messages. To exit press CTRL+C")
            try:
                channel.start_consuming()
                last_message_time = time.time()
            except ConnectionClosed:
                logger.error("Connection lost. Attempting to reconnect...")
                continue
            except StreamLostError as e:
                logger.error(f"Stream lost: {e}. Attempting to reconnect...")
                continue
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt. Shutting down...")
                break
            finally:
                if connection and connection.is_open:
                    connection.close()
                    logger.info("RabbitMQ connection closed")
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            time.sleep(5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Application terminated by user")
    except Exception as e:
        logger.critical(f"Critical error: {e}")
        send_telegram_message(f"redis_consumer: Critical error: {e}")
    finally:
        logger.info("Application shutdown complete")

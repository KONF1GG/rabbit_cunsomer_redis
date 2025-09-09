"""REDIS CONSUMER"""

import datetime
import json
import logging
import sys
import time
from threading import Thread
from typing import Optional, Dict, Any

import clickhouse_connect
import pika
import psycopg2
import pytz
import redis
import requests
from dotenv import dotenv_values
from pika.exceptions import ConnectionClosed, StreamLostError, AMQPConnectionError
from psycopg2.extras import RealDictCursor

# Constants
DEFAULT_TTL = 86400000  # 24 hours in milliseconds
CONNECTION_TIMEOUT = 10  # seconds
MONITORING_INTERVAL = 10  # seconds
INACTIVITY_THRESHOLD = 600  # 10 minutes in seconds
RBT_INACTIVITY_DAYS = 90  # days
TELEGRAM_TIMEOUT = 10  # seconds
TIMEZONE = "Etc/GMT-5"
RECONNECT_DELAY = 5  # seconds

# Field names for validation
FIELDS_TO_CONVERT = ["vlan", "mac", "ip_addr", "onu_mac"]

# Redis operation statuses
STATUS_INSERTED = "inserted"
STATUS_UPDATED = "updated"
STATUS_REPLACED = "replaced"
STATUS_FAILED_INSERT = "failed_to_insert"
STATUS_FAILED_UPDATE = "failed_to_update"
STATUS_FAILED_REPLACE = "failed_to_replace"
STATUS_SKIPPED = "skipped"
STATUS_ERROR = "error"
STATUS_SUCCESS = "success"


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

    # Отключаем избыточное логирование от pika
    logging.getLogger("pika").setLevel(logging.WARNING)

    return logging.getLogger(__name__)


logger = setup_logging()


class Config:
    """
    Класс для управления конфигурацией приложения.

    Загружает настройки из .env файла и предоставляет типизированный доступ
    к конфигурационным параметрам для всех компонентов системы.
    """

    def __init__(self) -> None:
        """Инициализация конфигурации из .env файла."""
        config = dotenv_values(".env")

        # Настройки RabbitMQ
        self.rabbit_host: str = self._get_required_config(config, "RABBIT_HOST")
        self.rabbit_user: str = self._get_required_config(config, "RABBIT_USER")
        self.rabbit_password: str = self._get_required_config(config, "RABBIT_PASSWORD")
        self.queue_name: str = self._get_required_config(config, "QUEUE_NAME")

        # Настройки Redis
        self.redis_host: str = self._get_required_config(config, "REDIS_HOST")
        self.redis_port: int = int(self._get_required_config(config, "REDIS_PORT"))
        self.redis_password: Optional[str] = config.get("REDIS_PASSWORD")

        # Настройки ClickHouse
        self.clickhouse_host: str = self._get_required_config(config, "HOST")
        self.clickhouse_port: Optional[str] = config.get("PORT")
        self.clickhouse_database: Optional[str] = config.get("DATABASE")
        self.clickhouse_user: str = self._get_required_config(config, "USER")
        self.clickhouse_password: str = self._get_required_config(config, "PASSWORD")

        # Настройки PostgreSQL
        self.postgres_host: str = self._get_required_config(config, "RBT_HOST")
        self.postgres_port: int = int(self._get_required_config(config, "RBT_PORT"))
        self.postgres_user: str = self._get_required_config(config, "RBT_USER")
        self.postgres_password: str = self._get_required_config(config, "RBT_PASSWORD")
        self.postgres_database: str = self._get_required_config(config, "RBT_DATABASE")

        # Настройки Telegram
        self.api_token: str = self._get_required_config(config, "API_TOKEN")
        self.chat_id: str = self._get_required_config(config, "CHAT_ID")

        # Настройки API
        self.api_endpoint: str = f"{config.get('API')}/check_and_correct_services/"

    def _get_required_config(self, config: Dict[str, Optional[str]], key: str) -> str:
        """
        Получает обязательный параметр конфигурации.
        """
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
    """
    Отправка уведомления в Telegram.

    Args:
        message: Текст сообщения для отправки
    """
    url = f"https://api.telegram.org/bot{app_config.api_token}/sendMessage"
    data = {"chat_id": app_config.chat_id, "text": message}
    try:
        response = requests.post(url, data=data, timeout=TELEGRAM_TIMEOUT)
        if response.status_code != 200:
            logger.error(
                "Ошибка отправки уведомления в Telegram: %s, %s",
                response.status_code,
                response.text,
            )
    except Exception as e:
        logger.error("Ошибка при отправке уведомления в Telegram: %s", e)


def check_enabled_services(key: str, fields_changed: bool = False) -> None:
    """
    Вызывает сервис для проверки включенных сервисов после успешного обновления данных login.

    Args:
        key: Ключ для проверки
        fields_changed: Флаг, указывающий, что изменились критические поля (onu_mac, mac, vlan)
    """
    if not key.startswith("login:"):
        return

    try:
        payload = {"key": key, "fields_changed": fields_changed}
        logger.info(
            "Calling API check_enabled_services with payload: %s for key: %s",
            payload,
            key,
        )

        response = requests.post(
            app_config.api_endpoint,
            json=payload,
            timeout=CONNECTION_TIMEOUT,
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            logger.info("Successfully checked enabled services for key: %s", key)
        else:
            logger.warning(
                "Failed to check enabled services for key %s: status %s, response: %s",
                key,
                response.status_code,
                response.text,
            )
    except Exception as e:
        logger.error("Error checking enabled services for key %s: %s", key, e)


def log_to_clickhouse(
    client,
    key: str,
    message_data: Dict[str, Any],
    status: str = STATUS_SUCCESS,
    error: Optional[str] = None,
) -> None:
    """
    Логирование событий в ClickHouse.

    Args:
        client: Клиент ClickHouse
        key: Ключ сообщения
        message_data: Данные сообщения
        status: Статус обработки
        error: Текст ошибки (если есть)
    """
    try:
        key_str = str(key) if key else ""
        message_data_str = json.dumps(message_data)
        status_str = str(status) if status else "unknown"
        error_str = str(error) if error else ""

        # Устанавливаем часовой пояс GMT+5
        timezone = pytz.timezone(TIMEZONE)
        timestamp = datetime.datetime.now(timezone).strftime("%Y-%m-%d %H:%M:%S")

        query = """
        INSERT INTO rabbitmq.logs (key, payload, status, error, timestamp)
        VALUES (%(key)s, %(payload)s, %(status)s, %(error)s, %(timestamp)s)
        """

        params = {
            "key": key_str,
            "payload": message_data_str,
            "status": status_str,
            "error": error_str,
            "timestamp": timestamp,
        }

        client.command(query, parameters=params)
        logger.debug("Logged to ClickHouse: key=%s, status=%s", key_str, status_str)
    except Exception as e:
        logger.error("Ошибка при записи в ClickHouse: %s", e)


def validate_and_convert_fields(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Проверяет и преобразует указанные поля в строковый формат.

    """
    for field in FIELDS_TO_CONVERT:
        if field in data and isinstance(data[field], (int, float)):
            data[field] = str(data[field])
            logger.debug("Converted field %s to string", field)

    return data


def check_rbt_status(phone: str) -> Optional[Dict[str, Any]]:
    """
    Проверяет статус RBT для заданного номера телефона в базе данных.

    Выполняет запрос к PostgreSQL базе данных для получения информации о пользователе.
    Проверяет актуальность данных (последняя активность не более 90 дней назад).
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
                three_months_ago = datetime.datetime.now() - datetime.timedelta(
                    days=RBT_INACTIVITY_DAYS
                )
                if last_seen > three_months_ago:
                    logger.debug("RBT status found for phone %s: active", phone)
                    return result
                else:
                    logger.debug(
                        "RBT status found for phone %s: inactive (last_seen: %s)",
                        phone,
                        last_seen,
                    )
                    return None
            else:
                logger.debug("RBT status not found for phone %s", phone)
                return None

    except Exception as e:
        error_msg = f"Error querying RBT database for phone {phone}: {e}"
        logger.error(error_msg)
        send_telegram_message(f"redis_consumer: {error_msg}")
        return None
    finally:
        if connection:
            connection.close()


def _parse_message_body(body: bytes) -> Optional[Dict[str, Any]]:
    """
    Парсит JSON из тела сообщения RabbitMQ.
    """
    try:
        message_data = json.loads(body)
        logger.info(
            "Processing message with key: %s", message_data.get("key", "unknown")
        )
        return message_data
    except json.JSONDecodeError as e:
        logger.error("Failed to decode JSON message: %s", e)
        return None


def _validate_message_data(message_data: Dict[str, Any]) -> Optional[str]:
    """
    Валидирует обязательные поля сообщения.

    """
    key = message_data.get("key")
    if not key:
        logger.error("Message missing required 'key' field")
        return None
    return key


def _enrich_phone_data(key: str, value: Dict[str, Any]) -> None:
    """
    Обогащает данные информацией о RBT для телефонных номеров.
    """
    if key.startswith("phone:"):
        phone = key.split(":", 1)[1]
        rbt_result = check_rbt_status(phone)
        if rbt_result:
            value["rbt"] = True
            value["auth_token"] = rbt_result["auth_token"]
            value["house_subscriber_id"] = rbt_result["house_subscriber_id"]
            logger.debug("Added RBT data for phone %s", phone)
        else:
            value["rbt"] = False
            logger.debug("No RBT data for phone %s", phone)


def _should_check_services(key: str, value: Dict[str, Any]) -> bool:
    """
    Определяет, нужно ли вызывать API для проверки сервисов.
    """
    # Проверяем, что ключ начинается с "login:"
    if not key.startswith("login:"):
        return False

    # Список полей, при наличии хотя бы одного из которых нужно вызвать API
    trigger_fields = ["servicecats", "speed", "password", "vlan", "onu_mac", "mac"]

    # Проверяем наличие хотя бы одного из полей
    present_trigger_fields = []
    for field in trigger_fields:
        if field in value:
            present_trigger_fields.append(field)

    if present_trigger_fields:
        logger.debug(
            "Trigger fields present for key %s: %s",
            key,
            ", ".join(present_trigger_fields),
        )
        return True

    return False


def _process_redis_operation(
    redis_conn,
    key: str,
    value: Dict[str, Any],
    create_if_not: bool,
    replace: bool,
    ttl: Optional[int],
) -> Dict[str, Any]:
    """
    Выполняет операцию с Redis (вставка, обновление или замена).

    Args:
        redis_conn: Клиент Redis
        key: Ключ для операции
        value: Данные для сохранения
        create_if_not: Создавать ключ, если не существует
        replace: Полностью заменить данные
        ttl: Время жизни ключа

    Returns:
        Словарь с результатом операции: {"status": str, "fields_changed": bool}
    """
    fields_changed = False

    if redis_conn.exists(key):
        # Получаем существующие данные для сравнения
        current_value = redis_conn.json().get(key)

        # Сравниваем поля onu_mac, mac, vlan только если они присутствуют в новом value
        fields_to_compare = ["onu_mac", "mac", "vlan"]
        changed_fields = []
        for field in fields_to_compare:
            # Проверяем только если поле присутствует в новом value
            if field in value:
                old_value = current_value.get(field)
                new_value = value.get(field)
                if old_value != new_value:
                    fields_changed = True
                    changed_fields.append(field)
                    logger.debug(
                        "Field %s changed for key %s: %s -> %s",
                        field,
                        key,
                        old_value,
                        new_value,
                    )

        # Логируем общую информацию об изменении критических полей
        if changed_fields:
            logger.info(
                "Critical fields changed for key %s: %s",
                key,
                ", ".join(changed_fields),
            )

        if replace:
            # Полностью заменить данные
            success = redis_conn.json().set(key, ".", value)
            if ttl and success:
                redis_conn.expire(key, ttl)
            status = STATUS_REPLACED if success else STATUS_FAILED_REPLACE
        else:
            # Обновить существующие данные
            current_value.update(value)
            success = redis_conn.json().set(key, ".", current_value)
            if ttl and success:
                redis_conn.expire(key, ttl)
            status = STATUS_UPDATED if success else STATUS_FAILED_UPDATE
    else:
        if create_if_not:
            success = redis_conn.json().set(key, ".", value)
            if ttl and success:
                redis_conn.expire(key, ttl)
            status = STATUS_INSERTED if success else STATUS_FAILED_INSERT
        else:
            status = STATUS_SKIPPED

    return {"status": status, "fields_changed": fields_changed}


def _handle_processing_result(
    ch, method, key: str, status: str, message_data: Dict[str, Any], clickhouse_client
) -> None:
    """
    Обрабатывает результат операции с Redis.

    Args:
        ch: Канал RabbitMQ
        method: Метод доставки
        key: Ключ сообщения
        status: Статус операции
        message_data: Данные сообщения
        clickhouse_client: Клиент ClickHouse
    """
    if status in {STATUS_INSERTED, STATUS_UPDATED, STATUS_REPLACED}:
        logger.info("Successfully %s key: %s", status, key)
        log_to_clickhouse(clickhouse_client, key, message_data, status=status)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    else:
        error_message = f"Failed to process message for key {key}: {status}"
        logger.info(error_message)
        log_to_clickhouse(
            clickhouse_client,
            key,
            message_data,
            status=STATUS_ERROR,
            error=error_message,
        )
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def process_message(
    ch, method, properties, body, redis_conn, clickhouse_client
) -> None:
    """
    Обработка сообщения из RabbitMQ.

    Args:
        ch: Канал RabbitMQ
        method: Метод доставки
        properties: Свойства сообщения (не используется)
        body: Тело сообщения
        redis_conn: Клиент Redis
        clickhouse_client: Клиент ClickHouse
    """
    global last_message_time
    last_message_time = time.time()

    # Парсинг сообщения
    message_data = _parse_message_body(body)
    if not message_data:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    # Валидация данных
    key = _validate_message_data(message_data)
    if not key:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    try:
        # Подготовка данных
        value = message_data.get("value", {})
        value = validate_and_convert_fields(value)

        # Обогащение данных для телефонных номеров
        _enrich_phone_data(key, value)

        # Получение параметров операции
        create_if_not = message_data.get("createIfNot", True)
        ttl = message_data.get("ttl", None)
        replace = message_data.get("replace", False)

        # Выполнение операции с Redis
        result = _process_redis_operation(
            redis_conn, key, value, create_if_not, replace, ttl
        )
        status = result["status"]
        fields_changed = result["fields_changed"]

        # Проверяем, нужно ли вызывать API для проверки сервисов
        should_call_api = _should_check_services(key, value)

        # Логируем информацию об изменении полей
        if fields_changed:
            logger.info("Critical fields (onu_mac, mac, vlan) changed for key: %s", key)
        else:
            logger.debug(
                "No changes in critical fields (onu_mac, mac, vlan) for key: %s", key
            )

        # Вызываем API если есть триггерные поля ИЛИ если изменились критические поля
        if should_call_api or fields_changed:
            if fields_changed and should_call_api:
                logger.info(
                    "Calling API for key %s - both trigger fields present AND critical fields changed",
                    key,
                )
            elif fields_changed:
                logger.info("Calling API for key %s - critical fields changed", key)
            else:
                logger.info("Calling API for key %s - trigger fields present", key)
            check_enabled_services(key, fields_changed)
        else:
            logger.debug(
                "Skipping API call for key %s - no trigger fields and no critical changes",
                key,
            )

        # Обработка результата
        _handle_processing_result(
            ch, method, key, status, message_data, clickhouse_client
        )

    except (ConnectionClosed, StreamLostError) as e:
        error_message = f"Connection lost while processing message for key {key}: {e}"
        logger.error(error_message)
        send_telegram_message(f"redis_consumer: {error_message}")
        log_to_clickhouse(
            clickhouse_client, key, message_data, status=STATUS_ERROR, error=str(e)
        )
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    except Exception as e:
        error_message = f"Error processing message for key {key}: {e}"
        logger.error(error_message)
        send_telegram_message(f"redis_consumer: {error_message}")
        log_to_clickhouse(
            clickhouse_client, key, message_data, status=STATUS_ERROR, error=str(e)
        )
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def setup_rabbitmq_channel():
    """
    Настройка канала RabbitMQ с автоматическим переподключением.
    """
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
                arguments={"x-message-ttl": DEFAULT_TTL},
            )
            logger.info("Successfully connected to RabbitMQ")
            return connection, channel
        except (ConnectionClosed, StreamLostError) as e:
            logger.error(
                "Connection lost during setup: %s. Retrying in %d seconds...",
                e,
                RECONNECT_DELAY,
            )
            time.sleep(RECONNECT_DELAY)
        except AMQPConnectionError as e:
            logger.error(
                "AMQP Connection error: %s. Retrying in %d seconds...",
                e,
                RECONNECT_DELAY,
            )
            time.sleep(RECONNECT_DELAY)
        except Exception as e:
            logger.error(
                "Unexpected connection error: %s. Retrying in %d seconds...",
                e,
                RECONNECT_DELAY,
            )
            time.sleep(RECONNECT_DELAY)


def monitor_connection(channel) -> None:
    """
    Мониторинг подключения и переподключение при необходимости.
    """
    global last_message_time
    while True:
        if time.time() - last_message_time > INACTIVITY_THRESHOLD:
            logger.warning(
                "No messages received in the last 10 minutes. Reconnecting..."
            )
            if channel.is_open:
                channel.stop_consuming()
            break
        time.sleep(MONITORING_INTERVAL)


def main() -> None:
    """
    Основная функция приложения.
    """
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
        logger.error("Failed to connect to ClickHouse: %s", e)
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
                except (ConnectionClosed, StreamLostError) as e:
                    logger.error("Connection lost in callback: %s. Will reconnect.", e)
                    # Пробрасываем исключение для переподключения
                    raise
                except Exception as e:
                    logger.error("Callback error: %s. Re-queueing message.", e)
                    try:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    except (ConnectionClosed, StreamLostError):
                        logger.error("Cannot nack message due to connection loss")
                        raise

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
            except ConnectionClosed as e:
                logger.error("Connection closed: %s. Attempting to reconnect...", e)
                continue
            except StreamLostError as e:
                logger.error("Stream lost: %s. Attempting to reconnect...", e)
                continue
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt. Shutting down...")
                break
            finally:
                if connection and connection.is_open:
                    connection.close()
                    logger.info("RabbitMQ connection closed")
        except Exception as e:
            logger.error("Unexpected error in main loop: %s", e)
            time.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Application terminated by user")
    except Exception as e:
        logger.critical("Critical error: %s", e)
        send_telegram_message(f"redis_consumer: Critical error: {e}")
    finally:
        logger.info("Application shutdown complete")

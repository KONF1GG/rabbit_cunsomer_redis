"""REDIS CONSUMER"""

import datetime
import json
import logging
import re
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
from pika.exceptions import (
    ConnectionClosed,
    StreamLostError,
    AMQPConnectionError,
    ChannelClosedByBroker,
)
from psycopg2.extras import RealDictCursor
from prometheus_client import start_http_server, Counter, Gauge, Histogram

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
STATUS_DELETED = "deleted"
STATUS_FAILED_DELETE = "failed_to_delete"


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

        # Настройки Exchange для отправки сообщений с request_id
        self.exchange_name: str = config.get("EXCHANGE_NAME", "responses")
        self.exchange_type: str = config.get("EXCHANGE_TYPE", "direct")

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

        # Metrics HTTP server port (Prometheus)
        _metrics_port_val = config.get("METRICS_PORT")
        self.metrics_port: int = int(_metrics_port_val) if _metrics_port_val else 8001

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

# Prometheus metrics (initialized after config)
messages_processed_total = Counter(
    "redis_consumer_messages_processed_total",
    "Total processed messages",
    ["status"],
)

messages_processing_duration_seconds = Histogram(
    "redis_consumer_processing_duration_seconds",
    "Message processing duration in seconds",
)

redis_operations_total = Counter(
    "redis_consumer_redis_operations_total",
    "Redis operations total",
    ["operation"],
)

messages_failed_total = Counter(
    "redis_consumer_messages_failed_total",
    "Total failed messages",
)

last_message_timestamp = Gauge(
    "redis_consumer_last_message_timestamp",
    "Unix timestamp of last processed message",
)

rabbitmq_connection_up = Gauge(
    "redis_consumer_rabbitmq_up",
    "RabbitMQ connection status (1 up, 0 down)",
)

redis_up = Gauge(
    "redis_consumer_redis_up",
    "Redis connection status (1 up, 0 down)",
)

clickhouse_up = Gauge(
    "redis_consumer_clickhouse_up",
    "ClickHouse connection status (1 up, 0 down)",
)


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
    # If ClickHouse client is not available, skip logging
    if not client:
        return
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

                # Check if last_seen is None (NULL in database)
                if last_seen is None:
                    logger.debug(
                        "RBT status found for phone %s: no last_seen data", phone
                    )
                    return None

                last_seen = datetime.datetime.fromtimestamp(last_seen)

                # Сравнение с текущей датой минус 90 дней
                three_months_ago = datetime.datetime.now() - datetime.timedelta(
                    days=RBT_INACTIVITY_DAYS
                )
                if last_seen > three_months_ago:
                    logger.debug("RBT status found for phone %s: active", phone)
                    # Ensure auth_token and house_subscriber_id are not None
                    if (
                        result.get("auth_token") is None
                        or result.get("house_subscriber_id") is None
                    ):
                        logger.debug(
                            "RBT status found for phone %s: missing required fields (auth_token or house_subscriber_id)",
                            phone,
                        )
                        return None
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
    Теперь требует обязательное поле operation со значением 'update' или 'delete'.
    """
    key = message_data.get("key")
    if not key:
        logger.error("Message missing required 'key' field")
        return None

    operation = message_data.get("operation")
    if not operation:
        logger.error("Message missing required 'operation' field")
        return None

    if operation not in ["update", "delete"]:
        logger.error("Invalid operation '%s'. Must be 'update' or 'delete'", operation)
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


def _delete_redis_key(redis_conn, key: str) -> bool:
    """
    Удаляет ключ из Redis. Возвращает True если ключ был удален или не существовал.
    """
    try:
        # delete() возвращает количество удаленных ключей
        deleted = redis_conn.delete(key)
        logger.debug(
            "Redis delete operation for key '%s': %s keys deleted", key, deleted
        )
        return True  # Успех независимо от того, существовал ключ или нет
    except Exception as e:
        logger.error("Error deleting key '%s' from Redis: %s", key, e)
        return False


def _parse_request_id(request_id: str) -> Optional[str]:
    """
    Парсит request_id и извлекает routing_key.

    Args:
        request_id: Строка в формате "routing_key_uuid"

    Returns:
        routing_key или None если не удалось распарсить
    """
    if not request_id:
        return None

    # UUID pattern для поиска UUID в конце строки
    uuid_pattern = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

    # Ищем UUID в конце строки
    match = re.search(uuid_pattern, request_id, re.IGNORECASE)
    if match:
        # Извлекаем routing_key (все что до UUID)
        routing_key = request_id[: match.start()].rstrip("_")
        logger.debug(
            "Parsed routing_key '%s' from request_id '%s'", routing_key, request_id
        )
        return routing_key

    logger.warning("Could not parse routing_key from request_id: %s", request_id)
    return None


def _create_response_message(
    original_message: Dict[str, Any],
    success: bool,
    error_message: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Создает простой ответ для отправки в exchange.

    Args:
        original_message: Исходное сообщение
        success: Успешность операции
        error_message: Сообщение об ошибке (если есть)

    Returns:
        Словарь с ответом
    """
    response = {
        "request_id": original_message.get("request_id"),
        "success": success,
        "timestamp": datetime.datetime.now(pytz.timezone(TIMEZONE)).isoformat(),
    }

    if error_message:
        response["error"] = error_message

    return response


def _publish_response_to_exchange(
    channel, response_data: Dict[str, Any], routing_key: str
) -> None:
    """
    Отправляет ответ в exchange с указанным routing_key.

    Args:
        channel: Канал RabbitMQ
        response_data: Данные ответа
        routing_key: Ключ маршрутизации
    """
    try:
        # Объявляем exchange если не существует
        channel.exchange_declare(
            exchange=app_config.exchange_name,
            exchange_type=app_config.exchange_type,
            durable=True,
        )

        # Отправляем ответ
        message_body = json.dumps(response_data)
        channel.basic_publish(
            exchange=app_config.exchange_name,
            routing_key=routing_key,
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Сделать сообщение постоянным
                content_type="application/json",
            ),
        )

        logger.info(
            "Published response to exchange '%s' with routing_key '%s', success: %s",
            app_config.exchange_name,
            routing_key,
            response_data.get("success"),
        )

    except Exception as e:
        logger.error("Failed to publish response to exchange: %s", e)
        send_telegram_message(
            f"redis_consumer: Failed to publish response to exchange: {e}"
        )


def _should_check_services(key: str, value: Dict[str, Any]) -> bool:
    """
    Определяет, нужно ли вызывать API для проверки сервисов.
    """
    # Проверяем, что ключ начинается с "login:"
    if not key.startswith("login:"):
        return False

    # Список полей, при наличии хотя бы одного из которых нужно вызвать API
    trigger_fields = [
        "servicecats",
        "speed",
        "password",
        "vlan",
        "onu_mac",
        "mac",
        "ip_addr",
    ]

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

        # Сравниваем критические поля onu_mac, mac, vlan, ip_addr только если они присутствуют в новом value
        fields_to_compare = ["onu_mac", "mac", "vlan", "ip_addr"]
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

        # Логируем общую информацию об изменении критических полей (onu_mac, mac, vlan, ip_addr)
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
    Обрабатывает результат операции с Redis и отправляет ответ в exchange если есть request_id.

    Args:
        ch: Канал RabbitMQ
        method: Метод доставки
        key: Ключ сообщения
        status: Статус операции
        message_data: Данные сообщения
        clickhouse_client: Клиент ClickHouse
    """
    success = status in {
        STATUS_INSERTED,
        STATUS_UPDATED,
        STATUS_REPLACED,
        STATUS_DELETED,
    }

    # Отправляем ответ в exchange если есть request_id
    request_id = message_data.get("request_id")
    if request_id:
        routing_key = _parse_request_id(request_id)
        if routing_key:
            error_message = None
            if not success:
                error_message = f"Failed to process message for key {key}: {status}"

            response_data = _create_response_message(
                original_message=message_data,
                success=success,
                error_message=error_message,
            )
            _publish_response_to_exchange(ch, response_data, routing_key)
        else:
            logger.warning(
                "Could not parse routing_key from request_id: %s", request_id
            )

    if success:
        logger.info("Successfully %s key: %s", status, key)
        log_to_clickhouse(clickhouse_client, key, message_data, status=status)
        # Metrics: successful message processed
        try:
            messages_processed_total.labels(status=status).inc()
            # Increment redis operation counters depending on status
            if status == STATUS_INSERTED:
                redis_operations_total.labels(operation="inserted").inc()
            elif status == STATUS_UPDATED:
                redis_operations_total.labels(operation="updated").inc()
            elif status == STATUS_REPLACED:
                redis_operations_total.labels(operation="replaced").inc()
            elif status == STATUS_DELETED:
                redis_operations_total.labels(operation="deleted").inc()
        except Exception:
            logger.debug("Failed to update success metrics")
        # update last message timestamp metric
        try:
            last_message_timestamp.set(time.time())
        except Exception:
            pass
        # Only ack if channel is still open
        try:
            if ch and ch.is_open:
                ch.basic_ack(delivery_tag=method.delivery_tag)
        except (ConnectionClosed, StreamLostError, ChannelClosedByBroker):
            logger.debug("Channel closed, cannot ack message")
            raise  # Re-raise to trigger reconnection
        except Exception as e:
            logger.warning("Error acking message: %s", e)
            raise
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
        try:
            messages_failed_total.inc()
            messages_processed_total.labels(status=STATUS_ERROR).inc()
        except Exception:
            logger.debug("Failed to update failure metrics")
        # Only nack if channel is still open
        try:
            if ch and ch.is_open:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except (ConnectionClosed, StreamLostError, ChannelClosedByBroker):
            logger.debug("Channel closed, cannot nack message")
            raise  # Re-raise to trigger reconnection
        except Exception as e:
            logger.warning("Error nacking message: %s", e)
            raise


def process_message(
    ch, method, properties, body, redis_conn, clickhouse_client
) -> None:
    """
    Обработка сообщения из RabbitMQ.
    Теперь поддерживает два типа операций: 'update' и 'delete'.

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
    # Metrics: update last message timestamp as soon as we start processing
    try:
        last_message_timestamp.set(time.time())
    except Exception:
        pass

    # Парсинг сообщения
    # Time the processing using Prometheus histogram
    with messages_processing_duration_seconds.time():
        message_data = _parse_message_body(body)
    if not message_data:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    # Валидация данных (теперь включает проверку operation)
    key = _validate_message_data(message_data)
    if not key:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    operation = message_data.get("operation")

    try:
        if operation == "delete":
            # Операция удаления - только ключ
            logger.info("Processing DELETE operation for key: %s", key)
            deleted_ok = _delete_redis_key(redis_conn, key)
            status = STATUS_DELETED if deleted_ok else STATUS_FAILED_DELETE

            # Обработка результата удаления
            _handle_processing_result(
                ch, method, key, status, message_data, clickhouse_client
            )

        elif operation == "update":
            # Операция обновления - текущий алгоритм
            logger.info("Processing UPDATE operation for key: %s", key)

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
                logger.info(
                    "Critical fields (onu_mac, mac, vlan, ip_addr) changed for key: %s",
                    key,
                )
            else:
                logger.debug(
                    "No changes in critical fields (onu_mac, mac, vlan, ip_addr) for key: %s",
                    key,
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

            # Обработка результата обновления
            _handle_processing_result(
                ch, method, key, status, message_data, clickhouse_client
            )

    except (ConnectionClosed, StreamLostError, ChannelClosedByBroker) as e:
        error_message = f"Connection/channel lost while processing {operation} operation for key {key}: {e}"
        logger.error(error_message)
        send_telegram_message(f"redis_consumer: {error_message}")
        log_to_clickhouse(
            clickhouse_client, key, message_data, status=STATUS_ERROR, error=str(e)
        )
        try:
            messages_failed_total.inc()
            messages_processed_total.labels(status=STATUS_ERROR).inc()
        except Exception:
            pass
        # Only try to nack if channel is still open
        try:
            if ch and ch.is_open:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        except Exception:
            # Channel is already closed, message will be requeued automatically
            logger.debug("Channel already closed, cannot nack message")
            pass

    except Exception as e:
        error_message = f"Error processing {operation} operation for key {key}: {e}"
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

            # Объявляем очередь
            channel.queue_declare(
                queue=app_config.queue_name,
                durable=True,
                arguments={"x-message-ttl": DEFAULT_TTL},
            )

            # Объявляем exchange для отправки сообщений с request_id
            channel.exchange_declare(
                exchange=app_config.exchange_name,
                exchange_type=app_config.exchange_type,
                durable=True,
            )

            logger.info(
                "Successfully connected to RabbitMQ and declared exchange '%s'",
                app_config.exchange_name,
            )
            try:
                rabbitmq_connection_up.set(1)
            except Exception:
                pass
            return connection, channel
        except (ConnectionClosed, StreamLostError) as e:
            logger.error(
                "Connection lost during setup: %s. Retrying in %d seconds...",
                e,
                RECONNECT_DELAY,
            )
            try:
                rabbitmq_connection_up.set(0)
            except Exception:
                pass
            time.sleep(RECONNECT_DELAY)
        except AMQPConnectionError as e:
            logger.error(
                "AMQP Connection error: %s. Retrying in %d seconds...",
                e,
                RECONNECT_DELAY,
            )
            try:
                rabbitmq_connection_up.set(0)
            except Exception:
                pass
            time.sleep(RECONNECT_DELAY)
        except Exception as e:
            logger.error(
                "Unexpected connection error: %s. Retrying in %d seconds...",
                e,
                RECONNECT_DELAY,
            )
            try:
                rabbitmq_connection_up.set(0)
            except Exception:
                pass
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
            try:
                # Check if channel is open before trying to stop consuming
                if channel and channel.is_open:
                    channel.stop_consuming()
            except (ConnectionClosed, StreamLostError) as e:
                logger.debug(
                    "Connection already closed while stopping consumption: %s", e
                )
            except Exception as e:
                # Handle all other exceptions (ChannelClosedByBroker, AssertionError, etc.)
                logger.debug(
                    "Error stopping consumption (channel may already be closed): %s", e
                )
            break
        time.sleep(MONITORING_INTERVAL)


def main() -> None:
    """
    Основная функция приложения.
    """
    global last_message_time

    logger.info("Starting Redis Consumer application...")

    # Start Prometheus metrics HTTP server first (should not block the app if CH is down)
    try:
        start_http_server(app_config.metrics_port)
        logger.info(
            "Prometheus metrics server started on port %s", app_config.metrics_port
        )
    except Exception as e:
        logger.warning("Failed to start Prometheus HTTP server: %s", e)

    # Try to connect to ClickHouse, but continue even if it fails
    clickhouse_client = None
    try:
        clickhouse_client = clickhouse_connect.get_client(
            host=app_config.clickhouse_host,
            username=app_config.clickhouse_user,
            password=app_config.clickhouse_password,
        )
        clickhouse_up.set(1)
        logger.info("Connected to ClickHouse")
    except Exception as e:
        logger.error("Failed to connect to ClickHouse: %s", e)
        try:
            clickhouse_up.set(0)
        except Exception:
            pass
        # Not returning here; app will continue without ClickHouse logging

    while True:
        try:
            connection, channel = setup_rabbitmq_channel()

            # If we have a redis client, flag it as up (best-effort)
            try:
                if redis_client.ping():
                    redis_up.set(1)
                else:
                    redis_up.set(0)
            except Exception:
                try:
                    redis_up.set(0)
                except Exception:
                    pass

            def callback(ch, method, properties, body):
                try:
                    process_message(
                        ch, method, properties, body, redis_client, clickhouse_client
                    )
                except (ConnectionClosed, StreamLostError, ChannelClosedByBroker) as e:
                    logger.error(
                        "Connection/channel lost in callback: %s. Will reconnect.", e
                    )
                    # Пробрасываем исключение для переподключения
                    raise
                except Exception as e:
                    logger.error("Callback error: %s. Re-queueing message.", e)
                    try:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    except (ConnectionClosed, StreamLostError, ChannelClosedByBroker):
                        logger.error(
                            "Cannot nack message due to connection/channel loss"
                        )
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
            except ChannelClosedByBroker as e:
                logger.error(
                    "Channel closed by broker: %s. Attempting to reconnect...", e
                )
                continue
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt. Shutting down...")
                break
            finally:
                try:
                    if connection and connection.is_open:
                        connection.close()
                        logger.info("RabbitMQ connection closed")
                except Exception as e:
                    logger.debug("Error closing connection: %s", e)
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

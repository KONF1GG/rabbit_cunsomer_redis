# Redis Consumer

Приложение для обработки сообщений из RabbitMQ и записи данных в Redis с интеграцией ClickHouse для логирования и PostgreSQL для проверки статуса RBT.

## Возможности

- **Обработка сообщений RabbitMQ**: Получение и обработка сообщений из очереди RabbitMQ
- **Работа с Redis**: Создание, обновление и замена JSON-данных в Redis
- **Интеграция с RBT**: Автоматическая проверка статуса RBT для телефонных номеров
- **Логирование в ClickHouse**: Сохранение логов обработки сообщений
- **Уведомления Telegram**: Отправка уведомлений об ошибках
- **Мониторинг подключений**: Автоматическое переподключение при сбоях
- **Структурированное логирование**: Логирование в файл и консоль

## Установка

1. Клонируйте репозиторий:
```bash
git clone <repository-url>
cd rabbit_consumer_redis
```

2. Установите зависимости:
```bash
pip install -r req.txt
```

3. Создайте файл конфигурации:
```bash
cp .env.example .env
```

4. Настройте параметры в файле `.env`

## Конфигурация

Все настройки задаются через файл `.env`. Пример конфигурации см. в файле `.env.example`.

### Обязательные параметры:

- **RabbitMQ**: RABBIT_HOST, RABBIT_USER, RABBIT_PASSWORD, QUEUE_NAME
- **Redis**: REDIS_HOST, REDIS_PORT
- **ClickHouse**: HOST, USER, PASSWORD
- **PostgreSQL**: RBT_HOST, RBT_PORT, RBT_USER, RBT_PASSWORD, RBT_DATABASE
- **Telegram**: API_TOKEN, CHAT_ID

## Использование

### Запуск приложения:
```bash
python main.py
```

### Формат сообщений RabbitMQ:

```json
{
    "key": "phone:1234567890",
    "value": {
        "name": "John Doe",
        "email": "john@example.com"
    },
    "createIfNot": true,
    "replace": false,
    "ttl": 3600
}
```

### Параметры сообщения:

- `key` (обязательный): Ключ для Redis
- `value` (обязательный): JSON-данные для сохранения
- `createIfNot` (по умолчанию: true): Создавать ключ, если он не существует
- `replace` (по умолчанию: false): Полностью заменить данные или обновить
- `ttl` (опционально): Время жизни ключа в секундах

## Особенности

### Обработка телефонных номеров

Для ключей, начинающихся с `phone:`, автоматически выполняется проверка статуса RBT в PostgreSQL базе данных. При обнаружении активного пользователя добавляются поля:

- `rbt`: true/false
- `auth_token`: токен авторизации (если найден)
- `house_subscriber_id`: идентификатор подписчика (если найден)

### Логирование

Приложение ведет логи в двух местах:
- **Файл**: `consumer.log` - все логи приложения
- **Консоль**: INFO уровень и выше
- **ClickHouse**: Логи обработки сообщений в таблице `rabbitmq.logs`

### Мониторинг

- Автоматическое переподключение к RabbitMQ при потере соединения
- Мониторинг активности (переподключение при отсутствии сообщений 10+ минут)
- Уведомления об ошибках через Telegram

## Docker

Для запуска в Docker используйте существующие файлы `Dockerfile` и `docker-compose.yaml`.

## Структура проекта

```
.
├── main.py              # Основной файл приложения
├── req.txt              # Зависимости Python
├── .env.example         # Пример конфигурации
├── Dockerfile           # Docker образ
├── docker-compose.yaml  # Docker Compose конфигурация
└── README.md           # Документация
```

## Логирование ошибок

Все ошибки логируются и отправляются в Telegram для быстрого реагирования. Настройте бота Telegram и получите токен для уведомлений.

## Производительность

- Обработка сообщений в однопоточном режиме с подтверждением
- Автоматический реквей сообщений при временных ошибках
- Graceful shutdown при получении сигнала прерывания

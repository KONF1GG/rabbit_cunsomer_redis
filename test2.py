import asyncio
import signal

from rstream import (
    AMQPMessage,
    Consumer,
    MessageContext,
    amqp_decoder,
)

STREAM = "to_redis"

async def consume():
    consumer = Consumer(
        host="192.168.110.52",
        port=5552,
        vhost="/",
        username="leo",
        password="12345",
    )

    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        stream = message_context.consumer.get_stream(message_context.subscriber_name)
        offset = message_context.offset
        print("Got message: {} from stream {}, offset {}".format(msg, stream, offset))

    try:
        await consumer.start()
        await consumer.subscribe(stream=STREAM, callback=on_message, decoder=amqp_decoder)
        await consumer.run()
    except asyncio.CancelledError:
        print("Consumer was cancelled.")
    except KeyboardInterrupt:
        print("Shutting down gracefully...")

# Running the consumer with asyncio
asyncio.run(consume())

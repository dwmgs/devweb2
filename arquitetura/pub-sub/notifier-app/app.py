import json
import logging
import telegram
import asyncio
from confluent_kafka import Consumer, KafkaError

    
api_key = '7558770276:AAGjPSHiuQF-G_LIYg-7jS4sIlMUA6LbQ50'
user_id = '6660744050'




async def send_telegram_message(message):
    bot = telegram.Bot(token=api_key)
    await bot.send_message(chat_id=user_id, text=message)


c = Consumer({
        'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
        'group.id': 'notifier-group',
        'client.id': 'client-1',
        'enable.auto.commit': True,
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['notificacao'])

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info(f'Fim da partição: {msg.topic()} {msg.partition()}')
            else:
                logging.error(f'Erro: {msg.error().str()}')
            continue

        data = json.loads(msg.value().decode('utf-8'))
        filename = data.get('filename')
        operation = data.get('operation')
        logging.warning(f"READING {filename}")
        notification_message = f'O arquivo {filename} foi {operation}.'
        asyncio.run(send_telegram_message(notification_message))
        logging.warning(f"ENDING {filename}")


except KeyboardInterrupt:
    pass
finally:
    c.close()
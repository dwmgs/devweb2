from PIL import Image, ImageOps
import os
from confluent_kafka import Consumer, KafkaError, Producer
import json
import logging
from time import sleep
from PIL import Image, ImageDraw

OUT_FOLDER = '/processed/text/'
NEW = '_text'
IN_FOLDER = "/appdata/static/uploads/"

def insert_text(path_file):
    pathname, filename = os.path.split(path_file)
    output_folder = pathname + OUT_FOLDER

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    original_image = Image.open(path_file)
    draw = ImageDraw.Draw(original_image)
    name, ext = os.path.splitext(filename)
    draw.text((0, 0), name, fill='white', font_size=35)
    original_image.save(output_folder + name + NEW + ext)

    notify_operation(filename, "text")

def notify_operation(filename, operation):
    topic = 'notificacao'
    message = {
        "filename": filename,
        "operation": operation
    }
    producer = Producer({'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093'})

    try:
        producer.produce(topic, value=json.dumps(message))
        producer.flush()
    except Exception as e:
        logging.error(f"Erro ao enviar mensagem para o Kafka: {e}")

#sleep(30)
### Consumer
c = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'text-group',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['image'])
#{"timestamp": 1649288146.3453217, "new_file": "9PKAyoN.jpeg"}

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            data = json.loads(msg.value())
            filename = data['new_file']
            logging.warning(f"READING {filename}")
            insert_text(IN_FOLDER + filename)
            logging.warning(f"ENDING {filename}")
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            logging.warning('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            logging.error('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass
finally:
    c.close()
import smtplib
from email.mime.text import MIMEText
from confluent_kafka import Consumer, KafkaError
import json

consumer = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'notificador-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['notificacao'])

SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
EMAIL_USER = 'atvweb2notf@gmail.com'
EMAIL_PASS = 'M8n7b6v5c4'

def send_email(to_email, subject, body):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = EMAIL_USER
    msg['To'] = to_email

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.send_message(msg)

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        elif not msg.error():
            data = json.loads(msg.value())
            operation = data['operation']
            recipient = data['email']
            subject = "Notificação de Processamento"
            body = f"O arquivo foi {operation}."
            send_email(recipient, subject, body)
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print("Fim da partição.")
        else:
            print(f"Erro: {msg.error().str()}")
finally:
    consumer.close()

import os
import logging
import json
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from urllib.parse import urljoin
from confluent_kafka import Producer, Consumer, KafkaError, Message, TopicPartition

app = FastAPI()

logging.basicConfig(level=logging.INFO)

kafka_producer = Producer({'bootstrap.servers': os.environ.get("KAFKA_BROKERS")})

kafka_consumers = {}
for topic in ["movie-events", "user-events", "payment-events"]:
    kafka_consumers[topic] = Consumer({
        'bootstrap.servers': os.environ.get("KAFKA_BROKERS"),
        'group.id': 'event-service',
        'auto.offset.reset': 'earliest',
    })
    kafka_consumers[topic].subscribe(topics=[topic])
    kafka_consumers[topic].assign([TopicPartition(topic, 0)])

def any_none(*args):
    return any([x is None for x in args])

def on_delivery(ke: KafkaError, msg: Message):
    if ke is not None:
        logging.error(f"Error sending message: {ke}")
    if msg.error() is not None:
        logging.error(f"Error producing message: {msg.error()}")
    message_notable_properties = [
        f"LEN={len(msg)}",
        f"HEADERS={msg.headers()}",
        f"KEY={msg.key()}",
        f"OFFSET={msg.offset()}",
        f"PARTITION={msg.partition()}",
        f"TOPIC={msg.topic()}",
        f"VALUE={msg.value()}",
    ]
    logging.info(f"Produced message: {', '.join(message_notable_properties)}")

def produce_message_and_then_consume_it(event_type, message):
    topic = f"{event_type}-events"
    kafka_producer.produce(topic, value=message, on_delivery=on_delivery)
    kafka_producer.flush()

    message = kafka_consumers[topic].poll(timeout=5)
    if message is None:
        logging.error(f"Failed to consume - no messages received")
        raise HTTPException(status_code=500, detail="Failed to consume message")
    if message.error() is not None:
        logging.error(f"Failed to consume message: {message.error()}")
        raise HTTPException(status_code=500, detail="Failed to consume message")
    message_notable_properties = [
        f"LEN={len(message)}",
        f"HEADERS={message.headers()}",
        f"KEY={message.key()}",
        f"OFFSET={message.offset()}",
        f"PARTITION={message.partition()}",
        f"TOPIC={message.topic()}",
        f"VALUE={message.value()}",
    ]
    logging.info(f"Consumed message: {', '.join(message_notable_properties)}")
    payload = json.loads(message.value())
    eventid = f"{event_type}"
    eventid += f"-{payload.get('movie_id')}" if payload.get("movie_id") else ""
    eventid += f"-{payload.get('user_id')}" if payload.get("user_id") else ""
    eventid += f"-{payload.get('action')}" if payload.get("action") else ""
    content={
        "status": "success",
        "partition": message.partition(),
        "offset": message.offset(),
        "event": {
            "id": eventid,
            "type": event_type,
            "timestamp": payload.get("timestamp"),
            "payload": payload
        }
    }
    logging.info(f"content: {content}")
    return JSONResponse(status_code=201, content=content)


@app.api_route("/api/events/health", methods=["GET"])
def health():
    return {"status": True}

@app.api_route("/api/events/movie", methods=["POST"])
async def create_movie_event(request: Request):
    logging.info(f"request: {request}")
    logging.info(f"request body: {await request.body()}")
    data = await request.json()
    movie_id = data.get("movie_id")
    title = data.get("title")
    action = data.get("action")
    user_id = data.get("user_id")

    if any_none(movie_id, title, action, user_id):
        raise HTTPException(status_code=400, detail="Invalid request body")

    return produce_message_and_then_consume_it("movie", json.dumps({
        "movie_id": movie_id,
        "title": title,
        "action": action,
        "user_id": user_id
    }).encode("utf-8"))


@app.api_route("/api/events/user", methods=["POST"])
async def create_user_event(request: Request):
    logging.info(f"request: {request}")
    logging.info(f"request body: {await request.body()}")
    data = await request.json()
    user_id = data.get("user_id")
    username = data.get("username")
    action = data.get("action")
    timestamp = data.get("timestamp")

    if any_none(user_id, username, action, timestamp):
        raise HTTPException(status_code=400, detail="Invalid request body")

    return produce_message_and_then_consume_it("user", json.dumps({
        "user_id": user_id,
        "username": username,
        "action": action,
        "timestamp": timestamp
    }).encode("utf-8"))

@app.api_route("/api/events/payment", methods=["POST"])
async def create_payment_event(request: Request):
    logging.info(f"request: {request}")
    logging.info(f"request body: {await request.body()}")
    data = await request.json()
    payment_id = data.get("payment_id")
    user_id = data.get("user_id")
    amount = data.get("amount")
    status = data.get("status")
    timestamp = data.get("timestamp")

    if any_none(payment_id, user_id, amount, status, timestamp):
        raise HTTPException(status_code=400, detail="Invalid request body")

    return produce_message_and_then_consume_it("payment", json.dumps({
        "payment_id": payment_id,
        "user_id": user_id,
        "amount": amount,
        "status": status,
        "timestamp": timestamp
    }).encode("utf-8"))

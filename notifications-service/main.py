from fastapi import FastAPI
from typing import List
from models import Notification, ErrorNotification
from aiokafka import AIOKafkaConsumer
from contextlib import asynccontextmanager
import asyncio, json

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Consumer for successful order confirmations
    order_confirmed_consumer = AIOKafkaConsumer(
        "order-confirmed",
        bootstrap_servers='kafka:9092',
        group_id="notifications-group",
        auto_offset_reset="earliest"
    )

    # Consumer for "product not found" error events
    product_not_found_consumer = AIOKafkaConsumer(
        "product_not_found_events",
        bootstrap_servers='kafka:9092',
        group_id="notifications-error-group",
        auto_offset_reset="earliest"
    )

    # Consumer for "out of stock" error events
    out_of_stock_consumer = AIOKafkaConsumer(
        "out_of_stock_events",
        bootstrap_servers='kafka:9092',
        group_id="notifications-stock-group",
        auto_offset_reset="earliest"
    )

    await order_confirmed_consumer.start()
    await product_not_found_consumer.start()
    await out_of_stock_consumer.start()

    tasks = [
        asyncio.create_task(consume_order_confirmed(order_confirmed_consumer)),
        asyncio.create_task(consume_error_events(product_not_found_consumer)),
        asyncio.create_task(consume_error_events(out_of_stock_consumer)),
    ]

    yield

    for task in tasks:
        task.cancel()
    await order_confirmed_consumer.stop()
    await product_not_found_consumer.stop()
    await out_of_stock_consumer.stop()

app = FastAPI(title="Notifications Service", lifespan=lifespan)

notifications_db: List[Notification] = []
error_notifications_db: List[ErrorNotification] = []

async def consume_order_confirmed(consumer: AIOKafkaConsumer):
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode('utf-8'))
            notification = Notification(
                order_id=data['order_id'],
                product_id=data['product_id'],
                message=f"Narudžbina {data['order_id']} za proizvod {data['product_id']} je uspešno potvrđena."
            )
            notifications_db.append(notification)
    except asyncio.CancelledError:
        pass

async def consume_error_events(consumer: AIOKafkaConsumer):
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode('utf-8'))
            error_notification = ErrorNotification(
                order_id=data['order_id'],
                product_id=data['product_id'],
                timestamp=data['timestamp'],
                error_reason=data['error_reason'],
                message=(
                    f"Narudžbina {data['order_id']} je odbijena. "
                    f"Razlog: {data['error_reason']} "
                    f"(Proizvod ID: {data['product_id']})."
                )
            )
            error_notifications_db.append(error_notification)
    except asyncio.CancelledError:
        pass

@app.get("/notifications", response_model=List[Notification])
def get_notifications():
    return notifications_db

@app.get("/notifications/errors", response_model=List[ErrorNotification])
def get_error_notifications():
    return error_notifications_db
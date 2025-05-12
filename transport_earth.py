from fastapi import FastAPI, Body, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from contextlib import asynccontextmanager
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
from typing import List
import time
import httpx
import asyncio

LINK_CODE_SEGMENT = "172.20.10.6:3050/CodeSegment"

APPLICATION_RECEIVE_RECEIPT = "172.20.10.2:8010/receiveReceipt"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "messages"
KAFKA_PARTITION = 0

MAX_BYTES = 2000
CONSUME_INTERVAL = 5
SEND_INTERVAL = 2

ready_to_send = True
last_offset = 0
id_seg = 0

segments_in_process = {}

app_loop = None

class Message(BaseModel):
    id: int = Field(..., example=1)
    username: str = Field(..., example="somerby")
    payload: str = Field(..., example="Hello world")
    timestamp: str = Field(..., example="12356532")

class Segment(BaseModel):
    id_seg: int
    messages: List[Message] = []

class Receipt(BaseModel):
    id_seg: int

@asynccontextmanager
async def lifespan(app: FastAPI):
    global app_loop
    app_loop = asyncio.get_event_loop()

    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id='topic_manager'
    )

    try:
        admin_client.delete_topics([KAFKA_TOPIC])
        print(f"Топик '{KAFKA_TOPIC}' удалён")
        time.sleep(2) 
    except Exception as e:
        print(f"Ошибка при удалении топика: {e}")

    try:
        topic = NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)
        admin_client.create_topics([topic])
        print(f"Топик '{KAFKA_TOPIC}' создан")
    except TopicAlreadyExistsError:
        print(f"Топик '{KAFKA_TOPIC}' создан")
    except Exception as e:
        print(f"Ошибка при создании топика: {e}")

    admin_client.close()

    yield

app = FastAPI(title="API транспортного уровня", lifespan=lifespan)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

async def SegmentProcess(segment: Segment):
    global segments_in_process
    for i in range(3):
        if segments_in_process[segment.id_seg]["status"] == "In process":
            await SendSegmentToLink(segment)
        else:
            print(f"Сегмент {segment.id_seg} успешно доставлен")
            del segments_in_process[segment.id_seg]
            return
        await asyncio.sleep(SEND_INTERVAL)
    if segments_in_process[segment.id_seg]["status"] == "In process":
        print(f"Сегмент {segment.id_seg} Доставить не удалось")
        await SendReceiptToApplication(segment.id_seg, False)
        del segments_in_process[segment.id_seg]
    else:
        print(f"Сегмент {segment.id_seg} успешно доставлен")
        del segments_in_process[segment.id_seg]

async def SendSegmentToLink(segment: Segment):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(f'http://{LINK_CODE_SEGMENT}', json=segment.model_dump())
        except httpx.HTTPError as e:
            print(f"Ошибка при отправке сегмента: {e}")

async def SendReceiptToApplication(id_seg: int, status: bool):
    async with httpx.AsyncClient() as client:
        for message in segments_in_process[id_seg]["segment"].messages:
            try:
                response = await client.post(
                    f'http://{APPLICATION_RECEIVE_RECEIPT}',
                    json={
                        "type": "receipt",
                        "payload": {
                            "msg_id": message.id,
                            "status": status
                        }
                    }
                )
            except httpx.HTTPError as e:
                print(f"Ошибка при отправке квитанции: {e}")

def Consume():
    global ready_to_send, last_offset, id_seg, segments_in_process, app_loop
    ready_to_send = False

    while not ready_to_send:
        print('Жду 5 секунд....')

        id_seg += 1

        time.sleep(CONSUME_INTERVAL)

        segment_id = int(time.time())
        segment = Segment(id_seg=id_seg)

        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id='segment_group',
            enable_auto_commit=False,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        tp = TopicPartition(KAFKA_TOPIC, KAFKA_PARTITION)
        consumer.assign([tp])

        consumer.seek(tp, last_offset)

        try:
            batch = consumer.poll(timeout_ms=500)

            for topic_partition, messages in batch.items():
                for message in messages:
                    msg_data = message.value
                    msg_obj = Message(**msg_data)
                    
                    temp_messages = segment.messages.copy()
                    temp_messages.append(msg_obj)
                    temp_segment = Segment(id_seg=segment.id_seg, messages=temp_messages)
                    temp_json = temp_segment.model_dump_json()

                    if len(temp_json.encode('utf-8')) <= MAX_BYTES:
                        segment.messages = temp_messages
                        last_offset = message.offset + 1
                    else:
                        consumer.seek(tp, last_offset)
                        break
        except Exception as e:
            print(f"Ошибка обработки: {e}")
        finally:
            if consumer.poll(timeout_ms=500):
                ready_to_send = False
            else:
                ready_to_send = True

            print(f"Сформирован сегмент: {segment}")
            segments_in_process[segment.id_seg] = {"status": "In process", "segment": segment}
            asyncio.run_coroutine_threadsafe(SegmentProcess(segment), app_loop)

            consumer.close()

@app.post("/Send", summary="Отправка сообщения", response_description="Сообщение обработано")
async def send_message(background_tasks: BackgroundTasks, msg: Message = Body(...)):
    producer.send(KAFKA_TOPIC, value=msg.model_dump(), partition=KAFKA_PARTITION)
    producer.flush()

    if ready_to_send:
        background_tasks.add_task(asyncio.to_thread, Consume)
    
    return {"status": "Message sent"}

@app.post("/TransferReceipt", summary="Отправка квитанции на Землю", response_description="Квитанция пришла на Землю")
async def transfer_receipt(rcp: Receipt = Body(...)):
    try:
        segments_in_process[rcp.id_seg]["status"] = "Done"
    except KeyError as e:
        raise HTTPException(status_code=404, detail="Нет такого сегмента в работе")
    await SendReceiptToApplication(rcp.id_seg, True)
    return {"status": "Ok"}
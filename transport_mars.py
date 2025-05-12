from fastapi import Body, FastAPI
from fastapi import Body, FastAPI
from main1 import Segment
import httpx

APPLICATION_RECEIVE = "172.20.10.2:8020/receive"
LINK_CODE_RECEIPT = "172.20.10.6:3050/CodeReceipt"

app = FastAPI(title="API транспортного уровня Марса")

async def SendMessagesToApplication(segment: Segment):
    async with httpx.AsyncClient() as client:
        for message in segment.messages:
            try:
                response = await client.post(
                    f'http://{APPLICATION_RECEIVE}',
                    json={
                        "id": message.id,
                        "username": message.username,
                        "data": message.payload,
                        "send_time": message.timestamp
                    }
                )
            except httpx.HTTPError as e:
                print(f"Ошибка при отправке сообщения: {e}")


@app.post("/TransferSegment", summary="Отправка сегмента на Марс", response_description="Сегмент пришел на Марс")
async def transfer_segment(sgm: Segment = Body(...)):
    await SendMessagesToApplication(sgm)
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(f'http://{LINK_CODE_RECEIPT}', json={"id_seg": sgm.id_seg})
        except httpx.HTTPError as e:
            print(f"Ошибка: {e}")
    return {"status": "Ok"}
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from broker.topic.manager import TopicManager
from client.consumer import Consumer
from client.producer import Producer

app = FastAPI()
topic_manager = TopicManager()
consumer = Consumer(topic_manager, group_id="default")
producer = Producer(topic_manager)


class ProduceRequest(BaseModel):
    value: str
    key: str | None = None


class TopicRequest(BaseModel):
    name: str
    num_partitions: int = 3


@app.post("/topics")
def create_topic(request: TopicRequest):
    topic_manager.create_topic(name=request.name, num_partitions=request.num_partitions)
    return JSONResponse(content={"status": "success"}, status_code=201)


@app.get("/topics")
def list_topics():
    return topic_manager.list_topics()


@app.delete("/topics/{name}")
def delete_topic(name: str):
    topic_manager.delete_topic(name=name)
    return JSONResponse(content={"status": "success"}, status_code=202)


@app.post("/produce/{topic}")
def produce(topic: str, request: ProduceRequest):
    key = request.key.encode("utf-8") if request.key else None
    offset, partition_id = producer.send(topic, request.value.encode("utf-8"), key=key)
    return JSONResponse(
        content={"topic": topic, "partition_id": partition_id, "offset": offset},
        status_code=201,
    )


@app.get("/consume/{topic}/{partition_id}")
def consume(topic: str, partition_id: int, limit: int = 10):
    message = consumer.poll(topic, partition_id, limit=limit)
    return JSONResponse(
        content=[
            {
                "offset": m.offset,
                "value": m.value.decode("utf-8"),
                "key": m.key.decode("utf-8") if m.key else None,
            }
            for m in message
        ],
        status_code=200,
    )


@app.post("/commit/{topic}/{partition_id}")
def commit(topic: str, partition_id: int):
    consumer.commit(topic, partition_id)
    return JSONResponse(content={"status": "committed"}, status_code=200)

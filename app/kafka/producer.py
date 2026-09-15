import json

from aiokafka import AIOKafkaProducer

from app.kafka.config import KAFKA_BOOTSTRAP_SERVERS


class KafkaProducer:
    def __init__(self):
        self._producer: AIOKafkaProducer | None = None

    async def start(self):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )

        await self._producer.start()

    async def stop(self):
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def send(self, topic: str, event: dict):
        if self._producer is None:
            raise RuntimeError("Kafka producer is not started")

        return await self._producer.send_and_wait(
            topic,
            event,
        )
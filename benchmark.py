import time
import asyncio

from confluent_kafka import Consumer
from aiokafka import AIOKafkaConsumer
from kafka import KafkaConsumer


class Benchmarker:
    def __init__(self, n=1_000_000, manual_commit=False, enable_auto_commit=False):
        self.n = n
        self.manual_commit = manual_commit
        self.enable_auto_commit = enable_auto_commit
        self.topic = "my-topic"
        self.config = {
            "bootstrap.servers": "localhost:9092",
            "auto.offset.reset": "earliest",
            "group.id": "foo",
            "enable.auto.commit": enable_auto_commit
        }
    
    def consume_by_cp_consumer(self):
        consumer = Consumer(self.config)
        consumer.subscribe([self.topic])

        total = 0
        cnt = -1
        while 1:
            st = time.time()
            msg = consumer.poll()
            cnt += 1

            if cnt == 0:
                continue

            if self.manual_commit and not self.enable_auto_commit:
                consumer.commit()
            
            if cnt == self.n:
                break

            total += time.time()-st
        
        consumer.close()

        print("ck", total, total / self.n)

    def consume_by_kf_consumer(self):
        self.config["group.id"] = "bar"
        consumer = KafkaConsumer(**{k.replace(".", "_"):v for k, v in self.config.items()})
        consumer.subscribe([self.topic])

        total = 0
        cnt = -1
        for msg in consumer:
            st = time.time()
            cnt += 1
            if cnt == 0:
                continue

            if self.manual_commit and not self.enable_auto_commit:
                consumer.commit()
                # consumer.commit_async()
            
            if cnt == self.n:
                break

            total += time.time()-st

        consumer.close()
        
        print("kf", total, total / self.n)

    async def consume_by_aio_consumer(self):
        self.config["group.id"] = "baz"
        consumer = AIOKafkaConsumer(**{k.replace(".", "_"):v for k, v in self.config.items()})
        consumer.subscribe([self.topic])

        # loop = asyncio.get_running_loop()
        await consumer.start()

        cnt = -1
        total = 0
        async for msg in consumer:
            st = time.time()
            cnt += 1

            if cnt == 0:
                continue

            if self.manual_commit and not self.enable_auto_commit:
                # loop.create_task(consumer.commit())
                await consumer.commit()

            if cnt == self.n:
                break

            total += time.time()-st
        print("ak", total, total / self.n)

        await consumer.stop()


if __name__ == "__main__":
    benchmarker = Benchmarker(manual_commit=True)
    benchmarker.consume_by_cp_consumer()
    benchmarker.consume_by_kf_consumer()
    asyncio.run(benchmarker.consume_by_aio_consumer())
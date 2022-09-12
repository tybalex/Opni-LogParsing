# Standard Library
import asyncio
import json
import logging
import os
import time

# Third Party
import pandas as pd
from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ConnectionTimeout
from elasticsearch.helpers import BulkIndexError, async_streaming_bulk
from opni_nats import NatsWrapper

# Local
from grouping_and_parsing import log_offline_parsing, log_online_matching
from masker import LogMasker

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")

ES_ENDPOINT = os.environ["ES_ENDPOINT"]
ES_USERNAME = os.environ["ES_USERNAME"]
ES_PASSWORD = os.environ["ES_PASSWORD"]


es = AsyncElasticsearch(
    [ES_ENDPOINT],
    port=9200,
    http_auth=(ES_USERNAME, ES_PASSWORD),
    http_compress=True,
    verify_certs=False,
    use_ssl=True,
    timeout=10,
    max_retries=5,
    retry_on_timeout=True,
)

nw = NatsWrapper()


async def consume_logs(parsing_logs_queue):
    async def subscribe_handler(msg):
        payload_data = msg.data.decode()
        # await parsing_logs_queue.put(pd.DataFrame(json.loads(payload_data), index=[0]))
        # await parsing_logs_queue.put(pd.json_normalize(json.loads(payload_data)))
        await parsing_logs_queue.put(json.loads(payload_data))

    await nw.subscribe(
        nats_subject="parsing_logs",
        nats_queue="workers",
        payload_queue=parsing_logs_queue,
        subscribe_handler=subscribe_handler,
    )


async def doc_generator(df):
    df["_op_type"] = "update"
    df["_index"] = "logs"
    doc_keywords = {"_op_type", "_index", "_id", "doc"}
    for index, document in df.iterrows():
        doc_dict = document[pd.notnull(document)].to_dict()
        doc_dict["doc"] = {}
        doc_dict_keys = list(doc_dict.keys())
        for k in doc_dict_keys:
            if not k in doc_keywords:
                doc_dict["doc"][k] = doc_dict[k]
                del doc_dict[k]
        yield doc_dict


async def parsing_logs(queue):
    masker = LogMasker()
    pending_list = []
    last_time = time.time()
    grouping_rule_to_template_dict = None
    TRAINING_INTERVAL = int(os.getenv("TRAINING_INTERVAL", "1800"))
    TRAINING_DATA_VOL = 200000
    logging.info(f"training interval : {TRAINING_INTERVAL}")
    while True:
        json_payload = await queue.get()
        pending_list.append(json_payload)
        this_time = time.time()
        if grouping_rule_to_template_dict is None:  # training
            # TODO training on new templates and merge templates by string similarity
            if (
                this_time - last_time >= TRAINING_INTERVAL
                or len(pending_list) >= TRAINING_DATA_VOL
            ):
                # "traininng"
                logging.info(f"========training {len(pending_list)} logs...")
                payload_data_df = pd.json_normalize(pending_list)
                _, grouping_rule_to_template_dict = log_offline_parsing(payload_data_df)
                pending_list = []
                last_time = this_time

        elif (
            this_time - last_time >= 1 or len(pending_list) >= 1000
        ):  # every seconds or every 1000 docs
            #  inferencing
            payload_data_df = pd.json_normalize(pending_list)
            logging.info(f"processing {len(pending_list)} logs...")
            matched_templates = log_online_matching(
                payload_data_df, grouping_rule_to_template_dict
            )
            pending_list = []
            last_time = this_time

            payload_data_df["ulp_template"] = matched_templates
            payload_data_df.drop(["_type"], axis=1, errors="ignore", inplace=True)
            payload_data_df.drop(["_version"], axis=1, errors="ignore", inplace=True)

            # temporarily use opensearch
            try:
                async for ok, result in async_streaming_bulk(
                    es, doc_generator(payload_data_df)
                ):
                    action, result = result.popitem()
            except (BulkIndexError, ConnectionTimeout) as exception:
                logging.error(exception)


async def init_nats():
    logging.info("Attempting to connect to NATS")
    await nw.connect()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    parsing_logs_queue = asyncio.Queue(loop=loop)
    nats_consumer_coroutine = consume_logs(parsing_logs_queue)
    parsing_logs_coroutine = parsing_logs(parsing_logs_queue)

    task = loop.create_task(init_nats())
    loop.run_until_complete(task)

    loop.run_until_complete(
        asyncio.gather(nats_consumer_coroutine, parsing_logs_coroutine)
    )
    try:
        loop.run_forever()
    finally:
        loop.close()

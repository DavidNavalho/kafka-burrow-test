import json
import logging
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Tuple
from urllib.parse import quote

import requests
from prometheus_client.parser import text_string_to_metric_families


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("es-metrics-indexer")

STATUS_TO_CODE = {
    "OK": 0,
    "WARN": 1,
    "ERR": 2,
    "STOP": 3,
    "STALL": 4,
    "NOTFOUND": 5,
    "UNKNOWN": 6,
}

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200").rstrip("/")
KAFKA_EXPORTER_URL = os.getenv("KAFKA_EXPORTER_URL", "http://kafka-exporter:9308/metrics")
BURROW_BASE_URL = os.getenv("BURROW_BASE_URL", "http://burrow:8000").rstrip("/")
INDEX_PREFIX = os.getenv("INDEX_PREFIX", "kafka-burrow-metrics")
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "5"))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "5"))
GROUP_REGEX = re.compile(os.getenv("GROUP_REGEX", r"^demo-.*"))
TOPIC_REGEX = re.compile(os.getenv("TOPIC_REGEX", r"^demo\..*"))

PREV_CONSUMED: Dict[str, Tuple[float, float]] = {}


def is_not_found_error(error: Exception) -> bool:
    if not isinstance(error, requests.HTTPError):
        return False
    response = error.response
    return response is not None and response.status_code == 404


def request_json(method: str, url: str, **kwargs) -> Dict:
    response = requests.request(method, url, timeout=REQUEST_TIMEOUT_SECONDS, **kwargs)
    response.raise_for_status()
    if response.text:
        return response.json()
    return {}


def wait_for_endpoint(url: str, label: str) -> None:
    while True:
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code < 500:
                LOGGER.info("%s is reachable at %s", label, url)
                return
        except Exception:  # pylint: disable=broad-except
            pass
        LOGGER.info("Waiting for %s at %s", label, url)
        time.sleep(2)


def ensure_index_template() -> None:
    template_url = f"{ELASTICSEARCH_URL}/_index_template/{INDEX_PREFIX}-template"
    body = {
        "index_patterns": [f"{INDEX_PREFIX}-*"],
        "template": {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
            },
            "mappings": {
                "dynamic": True,
                "properties": {
                    "@timestamp": {"type": "date"},
                    "metric": {"type": "keyword"},
                    "source": {"type": "keyword"},
                    "cluster": {"type": "keyword"},
                    "group": {"type": "keyword"},
                    "topic": {"type": "keyword"},
                    "partition": {"type": "integer"},
                    "status": {"type": "keyword"},
                    "status_code": {"type": "short"},
                    "value": {"type": "double"},
                }
            },
        },
    }
    request_json("PUT", template_url, json=body)
    LOGGER.info("Ensured Elasticsearch index template %s", template_url)


def parse_prometheus_samples(metrics_text: str) -> Iterable[Tuple[str, Dict[str, str], float]]:
    for family in text_string_to_metric_families(metrics_text):
        for sample in family.samples:
            yield sample.name, sample.labels, float(sample.value)


def scrape_kafka_exporter(ts_iso: str) -> List[Dict]:
    response = requests.get(KAFKA_EXPORTER_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    docs: List[Dict] = []
    lag_by_group: Dict[str, float] = defaultdict(float)
    consumed_by_group: Dict[str, float] = defaultdict(float)
    produced_by_topic: Dict[str, float] = defaultdict(float)

    for name, labels, value in parse_prometheus_samples(response.text):
        if name == "kafka_consumergroup_lag_sum":
            group = labels.get("consumergroup", "")
            topic = labels.get("topic", "")
            if GROUP_REGEX.match(group) and TOPIC_REGEX.match(topic):
                docs.append(
                    {
                        "@timestamp": ts_iso,
                        "source": "traditional",
                        "metric": "traditional_group_lag_sum",
                        "group": group,
                        "topic": topic,
                        "value": value,
                    }
                )
                lag_by_group[group] += value

        elif name == "kafka_consumergroup_current_offset_sum":
            group = labels.get("consumergroup", "")
            topic = labels.get("topic", "")
            if GROUP_REGEX.match(group) and TOPIC_REGEX.match(topic):
                docs.append(
                    {
                        "@timestamp": ts_iso,
                        "source": "traditional",
                        "metric": "traditional_group_consumed_offset_sum",
                        "group": group,
                        "topic": topic,
                        "value": value,
                    }
                )
                consumed_by_group[group] += value

        elif name == "kafka_topic_partition_current_offset":
            topic = labels.get("topic", "")
            partition = labels.get("partition", "0")
            if TOPIC_REGEX.match(topic):
                docs.append(
                    {
                        "@timestamp": ts_iso,
                        "source": "traditional",
                        "metric": "traditional_topic_partition_current_offset",
                        "topic": topic,
                        "partition": int(partition),
                        "value": value,
                    }
                )
                produced_by_topic[topic] += value

    now_ts = time.time()

    for group, total_lag in lag_by_group.items():
        docs.append(
            {
                "@timestamp": ts_iso,
                "source": "traditional",
                "metric": "traditional_group_total_lag",
                "group": group,
                "value": total_lag,
            }
        )

    # kafka_exporter omits zero-lag groups from lag_sum metrics; emit explicit zeros for active demo groups.
    for group in consumed_by_group:
        if group not in lag_by_group:
            docs.append(
                {
                    "@timestamp": ts_iso,
                    "source": "traditional",
                    "metric": "traditional_group_total_lag",
                    "group": group,
                    "value": 0.0,
                }
            )

    for group, total_consumed in consumed_by_group.items():
        docs.append(
            {
                "@timestamp": ts_iso,
                "source": "traditional",
                "metric": "traditional_group_total_consumed_offset",
                "group": group,
                "value": total_consumed,
            }
        )

        prev = PREV_CONSUMED.get(group)
        if prev is not None:
            prev_offset, prev_ts = prev
            elapsed = max(0.001, now_ts - prev_ts)
            rate = max(0.0, (total_consumed - prev_offset) / elapsed)
            docs.append(
                {
                    "@timestamp": ts_iso,
                    "source": "traditional",
                    "metric": "traditional_group_consume_rate",
                    "group": group,
                    "value": rate,
                }
            )
        PREV_CONSUMED[group] = (total_consumed, now_ts)

    for topic, produced in produced_by_topic.items():
        docs.append(
            {
                "@timestamp": ts_iso,
                "source": "traditional",
                "metric": "traditional_topic_produced_offset_sum",
                "topic": topic,
                "value": produced,
            }
        )

    return docs


def status_code(status: str) -> int:
    return STATUS_TO_CODE.get(status.upper(), STATUS_TO_CODE["UNKNOWN"])


def scrape_burrow(ts_iso: str) -> List[Dict]:
    docs: List[Dict] = []

    cluster_payload = request_json("GET", f"{BURROW_BASE_URL}/v3/kafka")
    clusters = cluster_payload.get("clusters", [])
    if not isinstance(clusters, list):
        return docs

    for cluster in [str(item) for item in clusters]:
        consumers_payload = request_json(
            "GET",
            f"{BURROW_BASE_URL}/v3/kafka/{quote(cluster, safe='')}/consumer",
        )
        groups = consumers_payload.get("consumers", [])
        if not isinstance(groups, list):
            continue

        for group in [str(item) for item in groups]:
            if not GROUP_REGEX.match(group):
                continue

            group_url = (
                f"{BURROW_BASE_URL}/v3/kafka/{quote(cluster, safe='')}/consumer/"
                f"{quote(group, safe='')}/lag"
            )
            try:
                group_payload = request_json("GET", group_url)
            except Exception as error:  # pylint: disable=broad-except
                if is_not_found_error(error):
                    LOGGER.info(
                        "Burrow group lag not found (likely transient), cluster=%s group=%s",
                        cluster,
                        group,
                    )
                    continue
                raise
            status_block = group_payload.get("status", {}) or {}

            group_status = str(status_block.get("status", "UNKNOWN")).upper()
            group_status_code = status_code(group_status)
            complete = float(status_block.get("complete", 0.0) or 0.0)

            maxlag_block = status_block.get("maxlag", {}) or {}
            maxlag_end = maxlag_block.get("end", {}) or {}
            max_lag = float(maxlag_end.get("lag", maxlag_block.get("current_lag", 0.0)) or 0.0)

            partitions = status_block.get("partitions", []) or []
            bad_partitions = 0

            for partition in partitions:
                topic = str(partition.get("topic", "unknown"))
                partition_id = int(partition.get("partition", -1))
                pstatus = str(partition.get("status", "UNKNOWN")).upper()
                pstatus_code = status_code(pstatus)
                current_lag = float(partition.get("current_lag", 0.0) or 0.0)

                if pstatus != "OK":
                    bad_partitions += 1

                docs.append(
                    {
                        "@timestamp": ts_iso,
                        "source": "burrow",
                        "cluster": cluster,
                        "metric": "burrow_partition_current_lag",
                        "group": group,
                        "topic": topic,
                        "partition": partition_id,
                        "status": pstatus,
                        "status_code": pstatus_code,
                        "value": current_lag,
                    }
                )

            docs.extend(
                [
                    {
                        "@timestamp": ts_iso,
                        "source": "burrow",
                        "cluster": cluster,
                        "metric": "burrow_group_status_code",
                        "group": group,
                        "status": group_status,
                        "status_code": group_status_code,
                        "value": float(group_status_code),
                    },
                    {
                        "@timestamp": ts_iso,
                        "source": "burrow",
                        "cluster": cluster,
                        "metric": "burrow_group_bad_partitions",
                        "group": group,
                        "value": float(bad_partitions),
                    },
                    {
                        "@timestamp": ts_iso,
                        "source": "burrow",
                        "cluster": cluster,
                        "metric": "burrow_group_max_lag",
                        "group": group,
                        "value": max_lag,
                    },
                    {
                        "@timestamp": ts_iso,
                        "source": "burrow",
                        "cluster": cluster,
                        "metric": "burrow_group_complete",
                        "group": group,
                        "value": complete,
                    },
                ]
            )

    return docs


def bulk_index(docs: List[Dict]) -> None:
    if not docs:
        return

    index_name = f"{INDEX_PREFIX}-{datetime.now(timezone.utc):%Y.%m.%d}"

    lines: List[str] = []
    for doc in docs:
        lines.append(json.dumps({"index": {"_index": index_name}}))
        lines.append(json.dumps(doc, separators=(",", ":")))

    payload = "\n".join(lines) + "\n"
    response = requests.post(
        f"{ELASTICSEARCH_URL}/_bulk",
        data=payload,
        headers={"Content-Type": "application/x-ndjson"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    body = response.json()
    if body.get("errors"):
        LOGGER.error("Bulk index reported errors")
    else:
        LOGGER.info("Indexed %d docs into %s", len(docs), index_name)


def run_once() -> None:
    timestamp = datetime.now(timezone.utc).isoformat()
    docs: List[Dict] = []

    try:
        docs.extend(scrape_kafka_exporter(timestamp))
    except Exception as error:  # pylint: disable=broad-except
        LOGGER.error("Failed scraping kafka-exporter: %s", error)

    try:
        docs.extend(scrape_burrow(timestamp))
    except Exception as error:  # pylint: disable=broad-except
        LOGGER.error("Failed scraping Burrow: %s", error)

    try:
        bulk_index(docs)
    except Exception as error:  # pylint: disable=broad-except
        LOGGER.error("Failed indexing docs: %s", error)


def main() -> None:
    wait_for_endpoint(f"{ELASTICSEARCH_URL}", "Elasticsearch")
    wait_for_endpoint(KAFKA_EXPORTER_URL, "kafka-exporter")
    wait_for_endpoint(f"{BURROW_BASE_URL}/v3/kafka", "Burrow")

    ensure_index_template()

    LOGGER.info("Starting index loop (interval=%ss)", POLL_INTERVAL_SECONDS)
    while True:
        run_once()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

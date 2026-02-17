import json
import logging
import math
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
KLAG_METRICS_URL = os.getenv("KLAG_METRICS_URL", "http://klag:8888/metrics")

KLAG_GROUP_STATE_CODE = {
    "stable": 0,
    "completing_rebalance": 1,
    "preparing_rebalance": 2,
    "empty": 3,
    "dead": 4,
    "unknown": 5,
}

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
            value = float(sample.value)
            if not math.isfinite(value):
                continue
            yield sample.name, sample.labels, value


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


def traditional_lag_status_code(total_lag: float) -> int:
    if total_lag < 100.0:
        return 0
    if total_lag < 1000.0:
        return 1
    return 2


def append_snapshot_count_docs(ts_iso: str, docs: List[Dict]) -> None:
    # Traditional snapshot: classify current total lag per group.
    traditional_lag_by_group: Dict[str, float] = {}
    for doc in docs:
        if (
            doc.get("source") == "traditional"
            and doc.get("metric") == "traditional_group_total_lag"
            and "group" in doc
        ):
            traditional_lag_by_group[str(doc["group"])] = float(doc.get("value", 0.0))

    traditional_counts = {0: 0, 1: 0, 2: 0}
    for total_lag in traditional_lag_by_group.values():
        traditional_counts[traditional_lag_status_code(total_lag)] += 1

    for code, status in ((0, "GREEN"), (1, "YELLOW"), (2, "RED")):
        docs.append(
            {
                "@timestamp": ts_iso,
                "source": "traditional",
                "metric": "traditional_group_health_count",
                "status": status,
                "status_code": code,
                "value": float(traditional_counts[code]),
            }
        )

    # Burrow snapshot: map status code into OK/WARN/ERR+ buckets per group.
    burrow_status_by_group: Dict[str, int] = {}
    for doc in docs:
        if (
            doc.get("source") == "burrow"
            and doc.get("metric") == "burrow_group_status_code"
            and "group" in doc
        ):
            code = int(doc.get("status_code", doc.get("value", STATUS_TO_CODE["UNKNOWN"])))
            burrow_status_by_group[str(doc["group"])] = code

    burrow_counts = {0: 0, 1: 0, 2: 0}
    for code in burrow_status_by_group.values():
        if code == 0:
            burrow_counts[0] += 1
        elif code == 1:
            burrow_counts[1] += 1
        else:
            burrow_counts[2] += 1

    for code, status in ((0, "OK"), (1, "WARN"), (2, "ERRPLUS")):
        docs.append(
            {
                "@timestamp": ts_iso,
                "source": "burrow",
                "metric": "burrow_group_health_count",
                "status": status,
                "status_code": code,
                "value": float(burrow_counts[code]),
            }
        )

    # KLag snapshot: use derived retention risk code; if multiple topics per group exist,
    # keep the worst current risk for that group.
    klag_risk_by_group: Dict[str, int] = {}
    for doc in docs:
        if (
            doc.get("source") == "klag"
            and doc.get("metric") == "klag_retention_risk_code"
            and "group" in doc
        ):
            group = str(doc["group"])
            code = int(doc.get("status_code", doc.get("value", 0.0)))
            klag_risk_by_group[group] = max(klag_risk_by_group.get(group, 0), code)

    klag_counts = {0: 0, 1: 0, 2: 0}
    for code in klag_risk_by_group.values():
        if code <= 0:
            klag_counts[0] += 1
        elif code == 1:
            klag_counts[1] += 1
        else:
            klag_counts[2] += 1

    for code, status in ((0, "GREEN"), (1, "YELLOW"), (2, "RED")):
        docs.append(
            {
                "@timestamp": ts_iso,
                "source": "klag",
                "metric": "klag_group_retention_count",
                "status": status,
                "status_code": code,
                "value": float(klag_counts[code]),
            }
        )


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


def klag_retention_risk(percent: float) -> Tuple[int, str]:
    if percent < 40.0:
        return 0, "GREEN"
    if percent < 80.0:
        return 1, "YELLOW"
    return 2, "RED"


def scrape_klag(ts_iso: str) -> List[Dict]:
    response = requests.get(KLAG_METRICS_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    docs: List[Dict] = []
    lag_max_by_group: Dict[str, float] = {}
    lag_min_by_group: Dict[str, float] = {}
    lag_sum_by_group_topic: Dict[Tuple[str, str], float] = defaultdict(float)
    lag_velocity_by_group_topic: Dict[Tuple[str, str], float] = {}
    allowed_metrics = {
        "klag_consumer_lag",
        "klag_consumer_lag_sum",
        "klag_consumer_lag_min",
        "klag_consumer_lag_max",
        "klag_consumer_lag_ms",
        "klag_consumer_lag_velocity",
        "klag_consumer_lag_retention_percent",
        "klag_consumer_group_state",
        "klag_consumer_committed_offset",
        "klag_partition_log_start_offset",
        "klag_partition_log_end_offset",
    }

    for name, labels, value in parse_prometheus_samples(response.text):
        if name not in allowed_metrics:
            continue

        group = labels.get("consumer_group", "")
        topic = labels.get("topic", "")
        partition = labels.get("partition")
        state = labels.get("state", "")

        if group and not GROUP_REGEX.match(group):
            continue
        if topic and not TOPIC_REGEX.match(topic):
            continue

        doc: Dict = {
            "@timestamp": ts_iso,
            "source": "klag",
            "metric": name,
            "value": value,
        }

        if group:
            doc["group"] = group
        if topic:
            doc["topic"] = topic
        if partition not in (None, ""):
            try:
                doc["partition"] = int(partition)
            except ValueError:
                pass
        if state:
            doc["state"] = state

        docs.append(doc)

        if name == "klag_consumer_group_state" and group and state:
            state_code = KLAG_GROUP_STATE_CODE.get(state.lower(), KLAG_GROUP_STATE_CODE["unknown"])
            docs.append(
                {
                    "@timestamp": ts_iso,
                    "source": "klag",
                    "metric": "klag_consumer_group_state_code",
                    "group": group,
                    "status": state.upper(),
                    "status_code": state_code,
                    "value": float(state_code),
                }
            )

        if name == "klag_consumer_lag_max" and group:
            lag_max_by_group[group] = value
        if name == "klag_consumer_lag_min" and group:
            lag_min_by_group[group] = value
        if name == "klag_consumer_lag" and group and topic:
            lag_sum_by_group_topic[(group, topic)] += value
        if name == "klag_consumer_lag_velocity" and group and topic:
            lag_velocity_by_group_topic[(group, topic)] = value

        if name == "klag_consumer_lag_retention_percent" and group:
            risk_code, risk_label = klag_retention_risk(value)
            risk_doc: Dict = {
                "@timestamp": ts_iso,
                "source": "klag",
                "metric": "klag_retention_risk_code",
                "group": group,
                "status": risk_label,
                "status_code": risk_code,
                "value": float(risk_code),
            }
            if topic:
                risk_doc["topic"] = topic
            docs.append(risk_doc)

    for group_topic in sorted(set(lag_sum_by_group_topic) | set(lag_velocity_by_group_topic)):
        group, topic = group_topic
        lag_sum = float(lag_sum_by_group_topic.get(group_topic, 0.0))
        velocity = float(lag_velocity_by_group_topic.get(group_topic, 0.0))

        # Estimated close time in seconds:
        # - finite value when lag is shrinking (negative velocity)
        # - 0 when lag already zero
        # - -1 when lag is not closing at current trend
        if lag_sum <= 0.0:
            estimate = 0.0
            closable = 1.0
        elif velocity < 0.0:
            estimate = lag_sum / abs(velocity)
            closable = 1.0
        else:
            estimate = -1.0
            closable = 0.0

        docs.append(
            {
                "@timestamp": ts_iso,
                "source": "klag",
                "metric": "klag_consumer_lag_time_to_close_estimate_seconds",
                "group": group,
                "topic": topic,
                "value": estimate,
            }
        )
        docs.append(
            {
                "@timestamp": ts_iso,
                "source": "klag",
                "metric": "klag_consumer_lag_closable",
                "group": group,
                "topic": topic,
                "value": closable,
            }
        )

    for group in sorted(set(lag_max_by_group) | set(lag_min_by_group)):
        max_lag = lag_max_by_group.get(group, 0.0)
        min_lag = lag_min_by_group.get(group, 0.0)
        docs.append(
            {
                "@timestamp": ts_iso,
                "source": "klag",
                "metric": "klag_consumer_lag_skew",
                "group": group,
                "value": max(0.0, max_lag - min_lag),
            }
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
        docs.extend(scrape_klag(timestamp))
    except Exception as error:  # pylint: disable=broad-except
        LOGGER.error("Failed scraping KLag: %s", error)

    try:
        append_snapshot_count_docs(timestamp, docs)
        bulk_index(docs)
    except Exception as error:  # pylint: disable=broad-except
        LOGGER.error("Failed indexing docs: %s", error)


def main() -> None:
    wait_for_endpoint(f"{ELASTICSEARCH_URL}", "Elasticsearch")
    wait_for_endpoint(KAFKA_EXPORTER_URL, "kafka-exporter")
    wait_for_endpoint(f"{BURROW_BASE_URL}/v3/kafka", "Burrow")
    wait_for_endpoint(KLAG_METRICS_URL, "KLag metrics")

    ensure_index_template()

    LOGGER.info("Starting index loop (interval=%ss)", POLL_INTERVAL_SECONDS)
    while True:
        run_once()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

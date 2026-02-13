import logging
import os
import time
from typing import Dict, List
from urllib.parse import quote

import requests
from prometheus_client import Counter, Gauge, start_http_server


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("burrow-exporter")

STATUS_TO_CODE = {
    "OK": 0,
    "WARN": 1,
    "ERR": 2,
    "STOP": 3,
    "STALL": 4,
    "NOTFOUND": 5,
    "UNKNOWN": 6,
}

GROUP_STATUS_CODE = Gauge(
    "burrow_group_status_code",
    "Burrow status code for a consumer group",
    ["cluster", "group"],
)
GROUP_STATUS_INFO = Gauge(
    "burrow_group_status_info",
    "Burrow status string represented as labels",
    ["cluster", "group", "status"],
)
GROUP_COMPLETE = Gauge(
    "burrow_group_complete",
    "Burrow completeness ratio for group evaluation",
    ["cluster", "group"],
)
GROUP_MAX_LAG = Gauge(
    "burrow_group_max_lag",
    "Burrow max lag seen in the group (from maxlag.end.lag)",
    ["cluster", "group"],
)
GROUP_BAD_PARTITIONS = Gauge(
    "burrow_group_bad_partitions",
    "Count of partitions in non-OK state for the group",
    ["cluster", "group"],
)
PARTITION_CURRENT_LAG = Gauge(
    "burrow_partition_current_lag",
    "Current lag by partition from Burrow status endpoint",
    ["cluster", "group", "topic", "partition", "partition_status"],
)
PARTITION_STATUS_CODE = Gauge(
    "burrow_partition_status_code",
    "Burrow partition status code",
    ["cluster", "group", "topic", "partition"],
)
SCRAPE_SUCCESS = Gauge(
    "burrow_exporter_scrape_success",
    "Whether the latest Burrow scrape succeeded",
)
SCRAPE_DURATION_SECONDS = Gauge(
    "burrow_exporter_scrape_duration_seconds",
    "Duration of the latest Burrow scrape",
)
SCRAPE_ERRORS_TOTAL = Counter(
    "burrow_exporter_scrape_errors_total",
    "Total number of Burrow scrape failures",
)


class BurrowClient:
    def __init__(self, base_url: str, timeout_seconds: float):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def get_json(self, path: str) -> Dict:
        response = requests.get(
            f"{self.base_url}{path}",
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()


def status_code(status: str) -> int:
    return STATUS_TO_CODE.get(status.upper(), STATUS_TO_CODE["UNKNOWN"])


def clear_dynamic_metrics() -> None:
    GROUP_STATUS_CODE.clear()
    GROUP_STATUS_INFO.clear()
    GROUP_COMPLETE.clear()
    GROUP_MAX_LAG.clear()
    GROUP_BAD_PARTITIONS.clear()
    PARTITION_CURRENT_LAG.clear()
    PARTITION_STATUS_CODE.clear()


def fetch_groups(client: BurrowClient, cluster: str) -> List[str]:
    payload = client.get_json(f"/v3/kafka/{quote(cluster, safe='')}/consumer")
    groups = payload.get("consumers", [])
    if not isinstance(groups, list):
        return []
    return [str(group) for group in groups]


def refresh_metrics(client: BurrowClient) -> None:
    started = time.monotonic()
    try:
        cluster_payload = client.get_json("/v3/kafka")
        clusters = cluster_payload.get("clusters", [])
        if not isinstance(clusters, list):
            clusters = []

        clear_dynamic_metrics()

        for cluster in [str(item) for item in clusters]:
            groups = fetch_groups(client, cluster)
            for group in groups:
                group_payload = client.get_json(
                    f"/v3/kafka/{quote(cluster, safe='')}/consumer/{quote(group, safe='')}/lag"
                )
                status_block = group_payload.get("status", {}) or {}

                group_status = str(status_block.get("status", "UNKNOWN")).upper()
                GROUP_STATUS_CODE.labels(cluster=cluster, group=group).set(
                    status_code(group_status)
                )
                GROUP_STATUS_INFO.labels(
                    cluster=cluster, group=group, status=group_status
                ).set(1)

                complete = float(status_block.get("complete", 0.0) or 0.0)
                GROUP_COMPLETE.labels(cluster=cluster, group=group).set(complete)

                maxlag_block = status_block.get("maxlag", {}) or {}
                maxlag_end = maxlag_block.get("end", {}) or {}
                maxlag = float(
                    maxlag_end.get("lag", maxlag_block.get("current_lag", 0.0)) or 0.0
                )
                GROUP_MAX_LAG.labels(cluster=cluster, group=group).set(maxlag)

                bad_partitions = 0
                partitions = status_block.get("partitions", []) or []
                for partition in partitions:
                    topic = str(partition.get("topic", "unknown"))
                    partition_id = str(partition.get("partition", "-1"))
                    partition_status = str(partition.get("status", "UNKNOWN")).upper()
                    current_lag = float(partition.get("current_lag", 0.0) or 0.0)

                    if partition_status != "OK":
                        bad_partitions += 1

                    PARTITION_CURRENT_LAG.labels(
                        cluster=cluster,
                        group=group,
                        topic=topic,
                        partition=partition_id,
                        partition_status=partition_status,
                    ).set(current_lag)
                    PARTITION_STATUS_CODE.labels(
                        cluster=cluster,
                        group=group,
                        topic=topic,
                        partition=partition_id,
                    ).set(status_code(partition_status))

                GROUP_BAD_PARTITIONS.labels(cluster=cluster, group=group).set(
                    bad_partitions
                )

        SCRAPE_SUCCESS.set(1)
    except Exception as error:  # pylint: disable=broad-except
        SCRAPE_SUCCESS.set(0)
        SCRAPE_ERRORS_TOTAL.inc()
        LOGGER.error("Scrape failed: %s", error)

    SCRAPE_DURATION_SECONDS.set(time.monotonic() - started)


def main() -> None:
    listen_port = int(os.getenv("EXPORTER_PORT", "9817"))
    poll_interval_seconds = float(os.getenv("POLL_INTERVAL_SECONDS", "5"))
    timeout_seconds = float(os.getenv("BURROW_HTTP_TIMEOUT_SECONDS", "4"))
    base_url = os.getenv("BURROW_BASE_URL", "http://burrow:8000")

    client = BurrowClient(base_url=base_url, timeout_seconds=timeout_seconds)
    start_http_server(listen_port)

    LOGGER.info(
        "Burrow exporter running on :%s, polling %s every %.1fs",
        listen_port,
        base_url,
        poll_interval_seconds,
    )

    while True:
        refresh_metrics(client)
        time.sleep(poll_interval_seconds)


if __name__ == "__main__":
    main()

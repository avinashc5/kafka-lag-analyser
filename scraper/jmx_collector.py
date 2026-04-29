import io
import math
from dataclasses import dataclass, field

import requests
from prometheus_client.parser import text_fd_to_metric_families


@dataclass
class JMXSample:
    metric_name: str
    value: float
    labels: dict = field(default_factory=dict)


class JMXCollector:
    """
    Fetches metrics from the Prometheus JMX Exporter HTTP endpoint and filters
    them down to the subset defined by target_prefixes.
    """

    def __init__(self, url: str, target_prefixes: list):
        self._url = url
        # Pre-lowercase the prefixes once so every collect() call is fast.
        self._prefixes = tuple(p.lower() for p in target_prefixes)

    def collect(self) -> list:
        """
        Returns a list of JMXSample for the current scrape.
        Raises requests.RequestException if the endpoint is unreachable.
        """
        resp = requests.get(self._url, timeout=10)
        resp.raise_for_status()
        return self._parse(resp.text)

    # ── internal ──────────────────────────────────────────────────────────────

    def _parse(self, text: str) -> list:
        samples = []
        for family in text_fd_to_metric_families(io.StringIO(text)):
            if not family.name.lower().startswith(self._prefixes):
                continue
            for s in family.samples:
                if s.value is None or math.isnan(s.value) or math.isinf(s.value):
                    continue
                samples.append(JMXSample(
                    metric_name=s.name,
                    value=float(s.value),
                    labels=dict(s.labels),
                ))
        return samples

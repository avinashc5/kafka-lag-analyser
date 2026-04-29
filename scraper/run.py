import logging
import signal
import time
from datetime import datetime, timezone

from .admin_collector import AdminCollector
from .config import (
    BATCH_SIZE,
    DB_PATH,
    JMX_EXPORTER_URL,
    JMX_TARGET_PREFIXES,
    KAFKA_BOOTSTRAP_SERVERS,
    POLL_INTERVAL_SECONDS,
)
from .database import Database
from .jmx_collector import JMXCollector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


class Scraper:
    def __init__(self):
        self._jmx   = JMXCollector(JMX_EXPORTER_URL, JMX_TARGET_PREFIXES)
        self._admin = AdminCollector(KAFKA_BOOTSTRAP_SERVERS)
        self._db    = Database(DB_PATH)
        self._batch: list = []
        self._running = False

    def run(self):
        self._admin.connect()
        self._db.connect()
        self._running = True

        signal.signal(signal.SIGINT,  self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        log.info(
            "Scraper started — interval=%ds  batch_size=%d  db=%s",
            POLL_INTERVAL_SECONDS, BATCH_SIZE, DB_PATH,
        )

        while self._running:
            self._poll()
            if self._running:
                time.sleep(POLL_INTERVAL_SECONDS)

        # Flush whatever is still in the buffer before exiting.
        self._flush()
        self._close()

    # ── poll ──────────────────────────────────────────────────────────────────

    def _poll(self):
        scraped_at = datetime.now(timezone.utc).isoformat()

        try:
            jmx_samples = self._jmx.collect()
        except Exception as exc:
            log.warning("JMX collection failed: %s", exc)
            jmx_samples = []

        try:
            admin_samples = self._admin.collect()
        except Exception as exc:
            log.warning("Admin collection failed: %s", exc)
            admin_samples = []

        self._batch.append({
            "scraped_at": scraped_at,
            "jmx":        jmx_samples,
            "admin":      admin_samples,
        })

        log.info(
            "Scraped  jmx=%-4d  lag_rows=%-3d  (batch %d/%d)",
            len(jmx_samples), len(admin_samples),
            len(self._batch), BATCH_SIZE,
        )

        if len(self._batch) >= BATCH_SIZE:
            self._flush()

    # ── flush / shutdown ───────────────────────────────────────────────────────

    def _flush(self):
        if not self._batch:
            return
        self._db.write_batch(self._batch)
        log.info("Flushed %d scrapes to DB", len(self._batch))
        self._batch.clear()

    def _handle_signal(self, *_):
        log.info("Shutdown signal received — will flush and exit after current poll")
        self._running = False

    def _close(self):
        self._admin.close()
        self._db.close()
        log.info("Scraper stopped")


def main():
    Scraper().run()


if __name__ == "__main__":
    main()

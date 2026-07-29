#!/usr/bin/env python3
"""
Pod-OS NYC Property Ingestor (One-time bulk load)
Pulls the July 2026 Supplemental Market Value Roll (Tax Class 1 + Class 2)
from NYC Department of Finance and stores structured events into
Evolutionary Neural Memory (A4_test).
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import sys
import zipfile
from pathlib import Path
from uuid import uuid4

import pandas as pd
import requests

from pod_os_client import Client, Config
from pod_os_client.config_env import config_from_env
from pod_os_client.message.constants import DataType
from pod_os_client.message.intents import IntentType
from pod_os_client.message.types import (
    BatchEventSpec,
    EventFields,
    Message,
    NeuralMemoryFields,
    PayloadFields,
    Tag,
)
from pod_os_client.message.utils import get_timestamp_from_datetime
from datetime import datetime, timezone

# === Configuration (override with environment variables) ===
CLIENT_NAME = "desmodromic"
GATEWAY_FQN = "zeroth.desmodromic.com"
TARGET_ACTOR = "A4_test@zeroth.desmodromic.com"
SOURCE = "nyc_dof_supplemental_roll_2026"
EVENT_TYPE = "property_record"
LOCATION = "nyc_property_roll"
LOCATION_SEPARATOR = "|"
MIN_MARKET_VALUE = 5_000_000          # Start high; lower later if needed
BATCH_CHUNK_SIZE = 50
RECEIVE_TIMEOUT = 180.0

# Official NYC DOF Supplemental Roll (July 2026)
CLASS1_URL = "https://nyc.gov/assets/finance/downloads/tar/fy27_tc1.zip"  # Placeholder – replace with real Class 1 ZIP
CLASS2_URL = "https://nyc.gov/assets/finance/downloads/tar/fy27_tc1.zip"  # Placeholder – replace with real Class 2 ZIP

# NOTE: Replace the two URLs above with the real links from:
# https://www.nyc.gov/site/finance/property/property-assessments.page
# under “Supplemental market value roll - July 2026”

log = logging.getLogger("desmodromic.nyc_property_ingestor")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def load_config() -> tuple[Config, str]:
    cfg = config_from_env()
    if not cfg.client_name:
        cfg.client_name = CLIENT_NAME
    if cfg.gateway_actor_name == "gateway":
        cfg.gateway_actor_name = os.getenv("PODOS_GATEWAY_FQN", GATEWAY_FQN)
    cfg.enable_concurrent_mode = True
    cfg.enable_reconnection = True
    cfg.receive_timeout = float(os.getenv("PODOS_RECEIVE_TIMEOUT", RECEIVE_TIMEOUT))
    target = os.getenv("PODOS_TARGET_ACTOR", TARGET_ACTOR)
    return cfg, target


def state_path() -> Path:
    return Path(os.getenv("NYC_STATE_FILE", "/tmp/nyc_property_state.json"))


def load_sent_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text())
        return set(data) if isinstance(data, list) else set()
    except Exception as e:
        log.warning("Could not load state: %s", e)
        return set()


def save_sent_ids(path: Path, sent: set[str]) -> None:
    try:
        path.write_text(json.dumps(sorted(sent)))
    except Exception as e:
        log.warning("Could not save state: %s", e)


def download_and_parse(url: str, tax_class: str) -> list[dict]:
    """Download ZIP or CSV and return list of property dicts."""
    log.info("Downloading %s ...", url)
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    # Handle ZIP
    if url.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            csv_name = [n for n in z.namelist() if n.endswith(".csv") or n.endswith(".txt")][0]
            with z.open(csv_name) as f:
                df = pd.read_csv(f, low_memory=False)
    else:
        df = pd.read_csv(io.BytesIO(resp.content), low_memory=False)

    log.info("Parsed %d rows for Tax Class %s", len(df), tax_class)
    records = []
    for _, row in df.iterrows():
        records.append(row.to_dict())
    return records


def make_unique_id(row: dict, tax_class: str) -> str:
    """Create deterministic unique ID. Prefer BBL if present."""
    boro = str(row.get("BOROUGH") or row.get("Borough") or row.get("boro") or "").strip()
    block = str(row.get("BLOCK") or row.get("Block") or "").strip()
    lot = str(row.get("LOT") or row.get("Lot") or "").strip()
    if boro and block and lot:
        return f"nyc|{tax_class}|{boro}-{block}-{lot}"
    # Fallback
    addr = str(row.get("ADDRESS") or row.get("Address") or row.get("owner_address") or uuid4())
    return f"nyc|{tax_class}|{addr[:80]}"


def row_to_spec(row: dict, tax_class: str) -> BatchEventSpec | None:
    try:
        # Try common column names for market/assessed value
        value = None
        for col in ["FULLVAL", "AVTOT", "Market Value", "ASSESSED_VALUE", "FULL_MARKET_VALUE"]:
            if col in row and pd.notna(row[col]):
                value = float(str(row[col]).replace(",", "").replace("$", ""))
                break

        if value is None or value < float(os.getenv("MIN_MARKET_VALUE", MIN_MARKET_VALUE)):
            return None

        unique_id = make_unique_id(row, tax_class)
        owner = str(row.get("OWNER") or row.get("Owner Name") or row.get("owner_name") or "Unknown")
        address = str(row.get("ADDRESS") or row.get("Address") or row.get("property_address") or "")

        now = datetime.now(timezone.utc)
        timestamp = get_timestamp_from_datetime(now)

        return BatchEventSpec(
            event=EventFields(
                unique_id=unique_id,
                owner="$sys",
                timestamp=timestamp,
                location=LOCATION,
                location_separator=LOCATION_SEPARATOR,
                type=EVENT_TYPE,
            ),
            tags=[
                Tag(key="owner_name", value=owner[:200], frequency=1),
                Tag(key="address", value=address[:200], frequency=1),
                Tag(key="borough", value=str(row.get("BOROUGH") or row.get("Borough") or ""), frequency=1),
                Tag(key="tax_class", value=tax_class, frequency=1),
                Tag(key="market_value", value=value, frequency=1),
                Tag(key="source", value=SOURCE, frequency=1),
                Tag(key="data_year", value="2026", frequency=1),
            ],
        )
    except Exception as e:
        log.warning("Skipping row: %s", e)
        return None


async def send_batch(client: Client, specs: list[BatchEventSpec], target: str, gateway: str, client_name: str) -> bool:
    if not specs:
        return True

    msg = Message(
        to=target,
        from_=f"{client_name}@{gateway}",
        client_name=client_name,
        message_id=str(uuid4()),
        intent=IntentType.StoreBatchEvents.name,
        neural_memory=NeuralMemoryFields(batch_events=specs),
        payload=PayloadFields(data=specs, mime_type="text/plain", data_type=DataType.RAW),
    )

    log.info("Sending batch of %d events → %s", len(specs), target)
    response = await client.send_message(msg)
    status = response.processing_status()
    if status != "OK":
        log.error("Batch failed: %s – %s", status, response.processing_message())
        return False
    log.info("Batch OK")
    return True


async def main() -> None:
    cfg, target_actor = load_config()
    state_file = state_path()
    sent_ids = load_sent_ids(state_file)

    log.info("=== NYC Property Ingestor (one-time) ===")
    log.info("Target: %s | Min value: %s", target_actor, os.getenv("MIN_MARKET_VALUE", MIN_MARKET_VALUE))

    all_specs: list[BatchEventSpec] = []

    # --- Class 1 ---
    try:
        rows = download_and_parse(os.getenv("CLASS1_URL", CLASS1_URL), "1")
        for row in rows:
            spec = row_to_spec(row, "1")
            if spec and spec.event.unique_id not in sent_ids:
                all_specs.append(spec)
    except Exception as e:
        log.error("Class 1 failed: %s", e)

    # --- Class 2 ---
    try:
        rows = download_and_parse(os.getenv("CLASS2_URL", CLASS2_URL), "2")
        for row in rows:
            spec = row_to_spec(row, "2")
            if spec and spec.event.unique_id not in sent_ids:
                all_specs.append(spec)
    except Exception as e:
        log.error("Class 2 failed: %s", e)

    log.info("Total new events to send: %d", len(all_specs))

    if not all_specs:
        log.info("Nothing new to send. Exiting.")
        return

    async with Client(cfg) as client:
        chunk_size = int(os.getenv("BATCH_CHUNK_SIZE", BATCH_CHUNK_SIZE))
        for i in range(0, len(all_specs), chunk_size):
            chunk = all_specs[i : i + chunk_size]
            ok = await send_batch(
                client,
                chunk,
                target_actor,
                cfg.gateway_actor_name,
                cfg.client_name,
            )
            if ok:
                for s in chunk:
                    sent_ids.add(s.event.unique_id)
                save_sent_ids(state_file, sent_ids)
            else:
                log.error("Stopping due to batch failure")
                break
            await asyncio.sleep(1.5)  # polite pause

    log.info("Ingestion complete. State saved to %s", state_file)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupted")
        sys.exit(0)

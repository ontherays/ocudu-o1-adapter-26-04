# SPDX-FileCopyrightText: Copyright (C) 2021-2026 Software Radio Systems Limited
# SPDX-License-Identifier: BSD-3-Clause-Open-MPI

"""
OSC-standard PM (Performance Management) file reporter for the O1 adapter.

This module implements the O-RAN-SC RANPM "file-based" PM path:

    gNB WebSocket metrics
        -> aggregate over a granularity period (default 60s)
        -> write a 3GPP TS 28.532 / 32.435 measData XML file to OCUDU_LOG_DIR
        -> serve it over HTTP (:5000 /files/<name>)  [Flask, wired in o1_adapter.py]
        -> VES fileReady notification (ves.send_file_ready)
        -> DFC pulls the file -> xml2json -> pm-producer -> pmlog -> InfluxDB(ran-pm-metrics)

Design notes
------------
* The XML structure is byte-compatible with the file that already ingests into
  `ran-pm-metrics` today (fileHeader/measCollec/measData/measInfo/measType/measValue,
  namespace 28.532#measData, fileFormatVersion V17.3). Only the *counter table* is
  widened. This keeps xml2json/pmlog parsing intact.
* Counters are declared in COUNTERS as (measType, source-key, aggregation, scale).
  To add a counter the SMO already selects, add one row here -- nothing else changes.
* Each counter's `source` is a dotted path resolved against the per-sample metric
  dict produced by extract_sample(); `agg` is "avg" or "sum" over the window.
* `status` marks whether the source is confirmed present in the WS payload
  ("confirmed") or needs a field that may not exist yet ("unverified") so you can
  enable rows incrementally and verify each lands in InfluxDB.
"""

from __future__ import annotations

import datetime
import logging
import os
import re
import subprocess
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


# --------------------------------------------------------------------------- #
# Counter catalogue  (measType  <->  WS source  <->  aggregation)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Counter:
    """One 3GPP measType and how to derive its value from the WS window."""
    meas_type: str            # 3GPP name emitted as <measType p="N">
    source: str               # dotted key into the flattened per-sample dict
    agg: str = "avg"          # "avg" | "sum"
    scale: float = 1.0        # multiply the aggregated value
    status: str = "confirmed" # "confirmed" | "unverified"


# The first three are your PROVEN counters (already reach ran-pm-metrics).
# RRC.ConnMean was already in the XML template. The rest are additive; enable
# incrementally and verify each in InfluxDB before relying on it.
COUNTERS: List[Counter] = [
    Counter("DRB.PdcpSduVolumeDL", "dl_brate",   agg="avg", status="confirmed"),
    Counter("DRB.PdcpSduVolumeUL", "ul_brate",   agg="avg", status="confirmed"),
    Counter("DRB.PdcpSduDelayDl",  "latency",    agg="avg", status="confirmed"),
    Counter("RRC.ConnMean",        "nof_ues",    agg="avg", status="confirmed"),
    # --- additive KPIs: sources present in typical OCUDU WS frames, verify first ---
    Counter("DRB.UEThpDl",         "dl_brate",   agg="avg", scale=0.001, status="unverified"),
    Counter("DRB.UEThpUl",         "ul_brate",   agg="avg", scale=0.001, status="unverified"),
    Counter("RRU.PrbUsedDl",       "prb_used_dl", agg="avg", status="unverified"),
    Counter("RRU.PrbTotDl",        "prb_tot_dl",  agg="avg", status="unverified"),
]


# --------------------------------------------------------------------------- #
# Per-sample extraction from a raw WS frame
# --------------------------------------------------------------------------- #
def extract_sample(data: Dict[str, Any]) -> Optional[Dict[str, float]]:
    """
    Reduce one raw WS JSON frame into a flat {source_key: value} sample.

    Mirrors the field access your proven handler used (cells[0].ue_list, dl_brate,
    ul_brate, latency, ptp_offset) and adds optional PRB fields when present.
    Returns None if the frame carries no cell metrics (so it isn't counted).
    """
    cells = data.get("cells")
    if not cells:
        return None
    cell0 = cells[0] if isinstance(cells, list) and cells else {}
    ue_list = cell0.get("ue_list") or []

    sample: Dict[str, float] = {
        "dl_brate": float(sum(ue.get("dl_brate", 0.0) for ue in ue_list)),
        "ul_brate": float(sum(ue.get("ul_brate", 0.0) for ue in ue_list)),
        "latency":  float(sum(ue.get("latency", 0.0) for ue in ue_list)),
        "nof_ues":  float(len(ue_list)),
        "ptp_offset": float(cell0.get("ptp_offset", 0.0) or 0.0),
    }
    # Optional PRB fields — only populated if the frame actually carries them.
    cm = cell0.get("cell_metrics") or {}
    for k_src, k_dst in (("prb_used_dl", "prb_used_dl"), ("prb_tot_dl", "prb_tot_dl")):
        if isinstance(cm.get(k_src), (int, float)):
            sample[k_dst] = float(cm[k_src])
    return sample


# --------------------------------------------------------------------------- #
# Sliding-window aggregator
# --------------------------------------------------------------------------- #
class PmAggregator:
    """Accumulate per-sample values over the granularity period."""

    def __init__(self, interval_s: int = 60):
        self.interval_s = interval_s
        self._reset(time.time())

    def _reset(self, now: float) -> None:
        self.start = now
        self.sums: Dict[str, float] = {}
        self.count = 0

    def add(self, sample: Dict[str, float]) -> None:
        for k, v in sample.items():
            self.sums[k] = self.sums.get(k, 0.0) + v
        self.count += 1

    def due(self, now: float) -> bool:
        return (now - self.start) >= self.interval_s

    def aggregate_and_reset(self, now: float) -> Optional[Dict[str, float]]:
        """Return {source_key: aggregated_value} for the window, or None if empty."""
        if self.count == 0:
            self._reset(now)
            return None
        out: Dict[str, float] = {}
        for c in COUNTERS:
            raw = self.sums.get(c.source)
            if raw is None:
                continue
            val = raw if c.agg == "sum" else raw / self.count
            out[c.source] = val
        # keep raw sums too, for any non-catalogue use
        self._agg_snapshot = dict(out)
        self._reset(now)
        return out


# --------------------------------------------------------------------------- #
# 3GPP TS 28.532 / 32.435 measData XML writer
# --------------------------------------------------------------------------- #
class PMXmlBuilder:
    """
    Build a 3GPP measData XML file. Structure identical to the proven template;
    the measType/measValue rows are generated from COUNTERS so the counter set
    is data-driven instead of hardcoded.
    """

    def __init__(
        self,
        vendor_name: str = "Software Radio Systems",
        dn_prefix: str = "ManagedElement=ran1,GNBDUFunction=du1",
        meas_obj_ldn: str = "NRCellDU=nrcelldu1",
    ):
        self.vendor_name = vendor_name
        self.dn_prefix = dn_prefix
        self.meas_obj_ldn = meas_obj_ldn

    def generate_xml(
        self,
        timestamp: float,
        duration_seconds: int,
        values: Dict[str, float],
        output_filepath: str,
        enabled_status: tuple = ("confirmed",),
    ) -> List[str]:
        """
        Write the measData XML. `values` is {source_key: aggregated_value}.
        Only counters whose status is in `enabled_status` AND whose source is
        present in `values` are emitted. Returns the list of measTypes written.
        """
        end_time = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc).replace(microsecond=0)
        begin_time = end_time - datetime.timedelta(seconds=duration_seconds)
        end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        begin_time_str = begin_time.strftime("%Y-%m-%dT%H:%M:%S+00:00")

        active = [
            c for c in COUNTERS
            if c.status in enabled_status and c.source in values
        ]
        if not active:
            logging.warning("PMXmlBuilder: no active counters have data this window; writing header-only file")

        meas_types = "\n".join(
            f'      <measType p="{i}">{c.meas_type}</measType>'
            for i, c in enumerate(active, start=1)
        )
        result_rows = "\n".join(
            f'        <r p="{i}">{int(round(values[c.source] * c.scale))}</r>'
            for i, c in enumerate(active, start=1)
        )

        xml_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<measDataFile xmlns="http://www.3gpp.org/ftp/specs/archive/28_series/28.532#measData">
  <fileHeader fileFormatVersion="V17.3" vendorName="{self.vendor_name}">
    <fileSender localDn="{self.dn_prefix}"/>
    <measCollec beginTime="{begin_time_str}"/>
  </fileHeader>
  <measData>
    <managedElement localDn="{self.dn_prefix}"/>
    <measInfo measInfoId="PM">
      <job jobId="1"/>
      <granPeriod duration="PT{duration_seconds}S" endTime="{end_time_str}"/>
      <repPeriod duration="PT{duration_seconds}S"/>
{meas_types}
      <measValue measObjLdn="{self.meas_obj_ldn}">
{result_rows}
      </measValue>
    </measInfo>
  </measData>
</measDataFile>"""

        with open(output_filepath, "w", encoding="utf-8") as f:
            f.write(xml_content)
        return [c.meas_type for c in active]


# --------------------------------------------------------------------------- #
# Optional PTP scrape (kept from your working version, off the hot path)
# --------------------------------------------------------------------------- #
def scrape_ptp_offset(syslog_path: str = "/var/log/syslog") -> float:
    """Best-effort ptp4l rms scrape; returns 0.0 if unavailable."""
    try:
        out = subprocess.check_output(["tail", "-n", "50", syslog_path], text=True, stderr=subprocess.DEVNULL)
        for line in reversed(out.splitlines()):
            if "ptp4l" in line and "rms" in line:
                m = re.search(r"rms\s+(-?\d+)", line)
                if m:
                    return float(m.group(1))
    except Exception as e:  # pylint: disable=broad-exception-caught
        logging.debug("PTP read skipped: %s", e)
    return 0.0


# --------------------------------------------------------------------------- #
# Top-level reporter: glue WS frames -> XML file -> fileReady
# --------------------------------------------------------------------------- #
class pm_reporter_filetype:
    """
    Feed raw WS frames via handle_frame(); every `interval_s` it writes one
    3GPP XML file and fires a VES fileReady via the injected `ves` object.
    """

    def __init__(
        self,
        ves,                                  # VesMessages instance (has send_file_ready)
        *,
        interval_s: int = 60,
        output_dir: Optional[str] = None,
        file_base_url: str = "http://192.168.206.105:5000/files",
        enabled_status: tuple = ("confirmed",),
        dn_prefix: str = "ManagedElement=ran1,GNBDUFunction=du1",
        meas_obj_ldn: str = "NRCellDU=nrcelldu1",
    ):
        self._ves = ves
        self.interval_s = interval_s
        self.output_dir = output_dir or os.environ.get("OCUDU_LOG_DIR", "/tmp")
        self.file_base_url = file_base_url.rstrip("/")
        self.enabled_status = enabled_status
        self._agg = PmAggregator(interval_s)
        self._builder = PMXmlBuilder(dn_prefix=dn_prefix, meas_obj_ldn=meas_obj_ldn)

    def handle_frame(self, data: Dict[str, Any]) -> None:
        """Call once per received WS JSON frame (already json.loads'd)."""
        sample = extract_sample(data)
        if sample is None:
            return
        self._agg.add(sample)

        now = time.time()
        if not self._agg.due(now):
            return

        values = self._agg.aggregate_and_reset(now)
        if not values:
            return

        end_dt = datetime.datetime.fromtimestamp(now, tz=datetime.timezone.utc)
        start_dt = end_dt - datetime.timedelta(seconds=self.interval_s)
        filename = f"A{start_dt.strftime('%Y%m%d.%H%M+0000')}-{end_dt.strftime('%H%M+0000')}_1_OCUDU.xml"
        filepath = os.path.join(self.output_dir, filename)

        try:
            written = self._builder.generate_xml(
                now, self.interval_s, values, filepath, enabled_status=self.enabled_status
            )
            logging.info("Generated %ds Bulk PM XML: %s (counters: %s)", self.interval_s, filepath, ", ".join(written))
        except OSError as e:
            logging.error("Failed to write PM XML %s: %s", filepath, e)
            return

        http_url = f"{self.file_base_url}/{filename}"
        try:
            self._ves.send_file_ready(filename, http_url)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logging.error("send_file_ready failed for %s: %s", filename, e)
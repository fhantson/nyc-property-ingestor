#!/usr/bin/env python3
"""
Pod-OS NYC Property Ingestor
Pulls the July 2026 Supplemental Market Value Roll (Class 1 + Class 2)
and stores structured events into Evolutionary Neural Memory (A4_test).
"""

# === Configuration (override with env vars) ===
TARGET_ACTOR = "A4_test@zeroth.desmodromic.com"
SOURCE = "nyc_dof_supplemental_roll_2026"
EVENT_TYPE = "property_record"
LOCATION = "nyc_property_roll"
LOCATION_SEPARATOR = "|"

# Download URLs (official NYC DOF page)
CLASS1_URL = "https://www.nyc.gov/assets/finance/downloads/..."  # exact link from assessments page
CLASS2_URL = "https://www.nyc.gov/assets/finance/downloads/..."  

MIN_VALUE = 5_000_000          # optional filter
BATCH_CHUNK_SIZE = 50
SLEEP_BETWEEN_BATCHES = 2      # polite pause

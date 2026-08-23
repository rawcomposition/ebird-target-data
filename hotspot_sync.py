#!/usr/bin/env python3
"""Opt-in sync of eBird hotspots to the OpenBirding Postgres database.

Enabled by setting DATABASE_URL in .env. If the database is only reachable
through an SSH tunnel, set DATABASE_TUNNEL_COMMAND to a command that opens it
(e.g. "ssh -N -L 19432:127.0.0.1:19432 root@example.com"); the tunnel is
started before connecting and terminated when the sync is closed.
"""

import shlex
import subprocess
import time
from datetime import datetime, timezone

UPSERT_SQL = """
    INSERT INTO hotspots (
        id, name, country_code, subnational1, subnational2, lat, lng,
        num_species, num_checklists, last_synced_at
    )
    VALUES (
        %(id)s, %(name)s, %(country_code)s, %(subnational1)s, %(subnational2)s,
        %(lat)s, %(lng)s, %(num_species)s, %(num_checklists)s, %(last_synced_at)s
    )
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        country_code = EXCLUDED.country_code,
        subnational1 = EXCLUDED.subnational1,
        subnational2 = EXCLUDED.subnational2,
        lat = EXCLUDED.lat,
        lng = EXCLUDED.lng,
        num_species = EXCLUDED.num_species,
        num_checklists = EXCLUDED.num_checklists,
        last_synced_at = EXCLUDED.last_synced_at,
        deleted_at = NULL,
        updated_at = CASE
            WHEN (hotspots.name, hotspots.country_code, hotspots.subnational1,
                  hotspots.subnational2, hotspots.lat, hotspots.lng,
                  hotspots.num_species, hotspots.num_checklists, hotspots.deleted_at)
                 IS DISTINCT FROM
                 (EXCLUDED.name, EXCLUDED.country_code, EXCLUDED.subnational1,
                  EXCLUDED.subnational2, EXCLUDED.lat, EXCLUDED.lng,
                  EXCLUDED.num_species, EXCLUDED.num_checklists, NULL)
            THEN now()
            ELSE hotspots.updated_at
        END
"""

SOFT_DELETE_SQL = """
    UPDATE hotspots
    SET deleted_at = now(), updated_at = now()
    WHERE (country_code = %(region)s OR subnational1 = %(region)s OR subnational2 = %(region)s)
      AND last_synced_at < %(run_start)s
      AND deleted_at IS NULL
"""


class HotspotSync:
    def __init__(self, conn, tunnel_process):
        self.conn = conn
        self.tunnel_process = tunnel_process
        self.run_start = datetime.now(timezone.utc)

    def sync_region(self, region: str, ebird_hotspots: list) -> tuple[int, int]:
        rows = [
            {
                "id": h.location_id,
                "name": h.name,
                "country_code": h.country_code,
                "subnational1": h.subnational1_code or None,
                "subnational2": h.subnational2_code or None,
                "lat": h.lat,
                "lng": h.lng,
                "num_species": h.total,
                "num_checklists": h.num_checklists,
                "last_synced_at": self.run_start,
            }
            for h in ebird_hotspots
        ]

        try:
            with self.conn.cursor() as cur:
                cur.executemany(UPSERT_SQL, rows)
                cur.execute(SOFT_DELETE_SQL, {"region": region, "run_start": self.run_start})
                deleted = cur.rowcount
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return len(rows), deleted

    def close(self):
        try:
            self.conn.close()
        finally:
            if self.tunnel_process:
                self.tunnel_process.terminate()
                self.tunnel_process.wait(timeout=5)


def create_hotspot_sync(env_vars: dict):
    """Returns a HotspotSync if DATABASE_URL is configured, otherwise None."""
    database_url = env_vars.get("DATABASE_URL")
    if not database_url:
        return None

    import psycopg

    tunnel_process = None
    tunnel_command = env_vars.get("DATABASE_TUNNEL_COMMAND")
    if tunnel_command:
        tunnel_process = subprocess.Popen(
            shlex.split(tunnel_command),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    last_error = None
    for _ in range(10):
        try:
            conn = psycopg.connect(database_url, connect_timeout=10)
            return HotspotSync(conn, tunnel_process)
        except psycopg.OperationalError as e:
            last_error = e
            if tunnel_process is None or tunnel_process.poll() is not None:
                break
            time.sleep(1)

    if tunnel_process:
        tunnel_process.terminate()
    raise Exception(f"Could not connect to hotspot database: {last_error}")

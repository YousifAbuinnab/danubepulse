"""Command-line entry points for migrations, demo ingestion, and replay."""

import argparse
import json
import logging
from datetime import date
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import select

from danubepulse.config import get_settings
from danubepulse.db import make_session_factory
from danubepulse.energy import ingest_energy, verify_energy
from danubepulse.energy_sources import REFERENCE
from danubepulse.models import Incident
from danubepulse.pipeline import (
    ingest_real_river,
    ingest_real_weather,
    replay_incident,
    run_demo,
)


def _upgrade() -> None:
    config = Config("alembic.ini")
    command.upgrade(config, "head")


def main() -> None:
    parser = argparse.ArgumentParser(prog="danubepulse")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("migrate", help="Apply database migrations.")
    energy_parser = subparsers.add_parser(
        "ingest-energy", help="Fetch ENTSO-E or import operator-verified Austrian exports."
    )
    energy_parser.add_argument("--date", type=date.fromisoformat, required=True)
    energy_parser.add_argument("--generation-file", type=Path)
    energy_parser.add_argument("--price-file", type=Path)
    energy_parser.add_argument("--load-forecast-file", type=Path)
    energy_parser.add_argument("--product", choices=("energy", "load_forecast"), default="energy")
    energy_parser.add_argument(
        "--accept-load-forecast-terms",
        action="store_true",
        help="One-run confirmation of A65 6.1.B source, attribution and reuse permission review.",
    )
    energy_parser.add_argument("--format", choices=("xml", "csv"), default="xml")
    energy_parser.add_argument("--reference", default=REFERENCE)
    energy_parser.add_argument(
        "--accept-terms",
        action="store_true",
        help="Confirm original source authenticity and permitted reuse for operator imports.",
    )
    demo_parser = subparsers.add_parser(
        "demo",
        help="Ingest labeled, deterministic synthetic fixtures.",
    )
    demo_parser.add_argument(
        "--force",
        action="store_true",
        help="Ingest another source snapshot.",
    )
    replay_parser = subparsers.add_parser("replay", help="Replay a detected incident.")
    replay_parser.add_argument("incident_id", nargs="?")
    river_parser = subparsers.add_parser(
        "ingest-real-river",
        help="Fetch current official Austrian Danube gauge readings.",
    )
    river_parser.add_argument("--force", action="store_true", help="Bypass the 15-minute cache.")
    weather_parser = subparsers.add_parser(
        "ingest-real-weather",
        help="Fetch hourly Open-Meteo historical weather for ingested Danube stations.",
    )
    weather_parser.add_argument("--start-date", type=date.fromisoformat)
    weather_parser.add_argument("--end-date", type=date.fromisoformat)
    weather_parser.add_argument("--model")
    weather_parser.add_argument(
        "--force", action="store_true", help="Bypass the stored-response cache."
    )
    args = parser.parse_args()
    if args.command == "ingest-energy":
        if args.product == "load_forecast":
            if not args.accept_load_forecast_terms:
                parser.error("Review A65 6.1.B permissions and pass --accept-load-forecast-terms.")
            if args.generation_file or args.price_file:
                parser.error("A65 imports cannot include generation or price files.")
        elif args.load_forecast_file or args.accept_load_forecast_terms:
            parser.error("A65 file/permission flags require --product load_forecast.")
    logging.basicConfig(level=get_settings().log_level)
    _upgrade()
    factory = make_session_factory()
    with factory() as session:
        if args.command == "migrate":
            print(json.dumps({"status": "migrated"}))
        elif args.command == "ingest-energy":
            if get_settings().data_mode != "real":
                parser.error("Energy ingestion requires DATA_MODE=real.")
            files = {
                "generation": args.generation_file,
                "price": args.price_file,
                "load_forecast": args.load_forecast_file,
            }
            payloads = {
                kind: path.read_text(encoding="utf-8-sig")
                for kind, path in files.items()
                if path is not None
            } or None
            run = ingest_energy(
                session,
                get_settings().bronze_dir,
                args.date,
                payloads=payloads,
                import_format=args.format,
                reference=args.reference,
                terms_accepted=args.accept_terms or get_settings().entsoe_terms_accepted,
                product=args.product,
                load_forecast_terms_accepted=args.accept_load_forecast_terms,
            )
            checks = verify_energy(session, run) if run.status != "failed" else []
            verified = bool(checks) and all(check["passed"] for check in checks)
            print(
                json.dumps(
                    {
                        "run_id": run.id,
                        "status": run.status,
                        "records": run.record_count,
                        "accepted": run.accepted_count,
                        "rejected": run.rejected_count,
                        "verified": verified,
                        "verification": checks,
                    }
                )
            )
            if not verified:
                raise SystemExit(1)
        elif args.command == "demo":
            if get_settings().data_mode != "demo":
                parser.error("Synthetic fixture ingestion requires DATA_MODE=demo.")
            payload = run_demo(session, get_settings().bronze_dir, force=args.force)
            print(json.dumps(payload, indent=2))
        elif args.command == "replay":
            if get_settings().data_mode != "demo":
                parser.error("Synthetic incident replay requires DATA_MODE=demo.")
            incident_id = args.incident_id
            if incident_id is None:
                incident = session.scalar(
                    select(Incident).where(Incident.status == "open").order_by(Incident.detected_at)
                )
                if incident is None:
                    parser.error("No open incident is available to replay.")
                incident_id = incident.id
            result = replay_incident(session, incident_id)
            print(json.dumps({"incident_id": result.incident_id, "status": result.status}))
        elif args.command in {"ingest-real-river", "ingest-real-weather"}:
            if get_settings().data_mode != "real":
                parser.error("Live-source ingestion requires DATA_MODE=real.")
            if args.command == "ingest-real-river":
                run = ingest_real_river(session, get_settings().bronze_dir, force=args.force)
            else:
                run = ingest_real_weather(
                    session,
                    get_settings().bronze_dir,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    models=args.model,
                    force=args.force,
                )
            print(
                json.dumps(
                    {
                        "run_id": run.id,
                        "source_id": run.source_id,
                        "status": run.status,
                        "received_count": run.record_count,
                        "accepted_count": run.accepted_count,
                        "rejected_count": run.rejected_count,
                        "checksum": run.checksum,
                    },
                    indent=2,
                )
            )

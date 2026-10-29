"""Bounded ENTSO-E A75/A44/A65 access and strict interval parsing.

The official Postman collection is linked from ENTSO-E's API integration sitemap.
No APG endpoint is assumed: APG distributes its API specification on request.
"""

import csv
import io
import math
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from danubepulse.time_utils import parse_utc

AUSTRIA = "10YAT-APG------L"
ENDPOINT = "https://web-api.tp.entsoe.eu/api"
SOURCE_ID = "entsoe-energy"
LOAD_FORECAST_SOURCE_ID = "entsoe-load-forecast"
REFERENCE = "https://transparency.entsoe.eu/"
PRODUCTION_TYPES = {
    "B01": "Biomass",
    "B02": "Fossil brown coal/lignite",
    "B03": "Fossil coal-derived gas",
    "B04": "Fossil gas",
    "B05": "Fossil hard coal",
    "B06": "Fossil oil",
    "B07": "Fossil oil shale",
    "B08": "Fossil peat",
    "B09": "Geothermal",
    "B10": "Hydro pumped storage (generation)",
    "B11": "Hydro run-of-river and poundage",
    "B12": "Hydro water reservoir",
    "B13": "Marine",
    "B14": "Nuclear",
    "B15": "Other renewable",
    "B16": "Solar",
    "B17": "Waste",
    "B18": "Wind offshore",
    "B19": "Wind onshore",
    "B20": "Other",
    "B25": "Energy storage",
}
Kind = Literal["generation", "price", "load_forecast"]
Product = Literal["energy", "load_forecast"]


def product_source(product: Product) -> str:
    if product not in {"energy", "load_forecast"}:
        raise EnergySourceError("Unsupported energy data product.")
    return LOAD_FORECAST_SOURCE_ID if product == "load_forecast" else SOURCE_ID


def product_gold(product: Product) -> str:
    product_source(product)
    return (
        "gold_real_load_forecast_daily" if product == "load_forecast" else "gold_real_energy_daily"
    )


class EnergySourceError(ValueError):
    """Explicit source/schema failure; messages never contain a token-bearing URL."""


@dataclass(frozen=True)
class EnergyPoint:
    start: datetime
    end: datetime
    metric: str
    production_type: str
    value: float | None
    unit: str
    document_id: str
    revision: int
    published_at: datetime

    @property
    def hours(self) -> float:
        return (self.end - self.start).total_seconds() / 3600


def strict_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise EnergySourceError("Energy timestamps require an explicit UTC offset.")
    return parse_utc(parsed)


def validate_points(points: list[EnergyPoint]) -> None:
    if not points:
        raise EnergySourceError("No supported Austrian energy points.")
    seen: dict[tuple, EnergyPoint] = {}
    for point in sorted(points, key=lambda p: (p.metric, p.production_type, p.start)):
        seconds = (point.end - point.start).total_seconds()
        if seconds not in {900, 1800, 3600} or point.start.timestamp() % seconds:
            raise EnergySourceError("Expected aligned 15, 30 or 60 minute intervals.")
        if not point.document_id or len(point.document_id) > 160 or point.revision < 1:
            raise EnergySourceError("Missing document identity or invalid provider revision.")
        if point.metric not in {"generation", "price", "load_forecast"}:
            raise EnergySourceError("Unsupported energy metric.")
        if point.metric == "generation":
            if point.production_type not in PRODUCTION_TYPES or point.unit != "MAW":
                raise EnergySourceError("Generation requires a documented PSR type and MAW (MW).")
        elif point.metric == "load_forecast":
            if point.production_type != "load" or point.unit != "MAW":
                raise EnergySourceError("Day-ahead load forecast requires load and MAW (MW).")
        elif point.production_type != "price" or point.unit != "EUR/MWh":
            raise EnergySourceError("Day-ahead price must be EUR/MWh.")
        if point.value is not None and (
            not math.isfinite(point.value) or (point.metric != "price" and point.value < 0)
        ):
            raise EnergySourceError("Non-finite value or negative generation/load forecast.")
        key = (point.metric, point.production_type)
        previous = seen.get(key)
        if previous and previous.end > point.start:
            raise EnergySourceError("Duplicate or overlapping intervals in provider response.")
        seen[key] = point


def fetch_xml(token: str | None, kind: Kind, start: datetime, end: datetime) -> bytes:
    if kind not in {"generation", "price", "load_forecast"}:
        raise EnergySourceError("Unsupported energy metric.")
    if not token:
        raise EnergySourceError("Set ENTSOE_API_TOKEN after obtaining ENTSO-E API approval.")
    start, end = parse_utc(start), parse_utc(end)
    if not timedelta(0) < end - start <= timedelta(days=1):
        raise EnergySourceError("Fetch one UTC day or less per request.")
    params = {
        "securityToken": token,
        "documentType": {"generation": "A75", "price": "A44", "load_forecast": "A65"}[kind],
        "periodStart": start.strftime("%Y%m%d%H%M"),
        "periodEnd": end.strftime("%Y%m%d%H%M"),
    }
    if kind == "generation":
        params.update(in_Domain=AUSTRIA, processType="A16", curveType="A01")
    elif kind == "price":
        params.update(in_Domain=AUSTRIA, out_Domain=AUSTRIA)
    else:
        params.update(outBiddingZone_Domain=AUSTRIA, processType="A01", curveType="A01")
    request = Request(ENDPOINT + "?" + urlencode(params), headers={"Accept": "application/xml"})
    # Source serialization and spacing bound this deployment to <=60 requests/minute.
    time.sleep(1)
    try:
        with urlopen(request, timeout=60) as response:
            body = response.read(20_000_001)
    except HTTPError as error:
        # Do not log or chain urllib errors: their URL contains securityToken.
        raise EnergySourceError(f"ENTSO-E HTTP {error.code}; inspect account/limits.") from None
    except (URLError, TimeoutError):
        raise EnergySourceError("ENTSO-E connection failed; retry later.") from None
    if len(body) > 20_000_000:
        raise EnergySourceError("ENTSO-E response exceeds 20 MB safety limit.")
    return body


def parse_xml(body: bytes, kind: Kind, start: datetime, end: datetime) -> list[EnergyPoint]:
    if len(body) > 20_000_000 or b"<!" in body or b"\0" in body:
        raise EnergySourceError("Oversized XML or forbidden XML declarations/entities.")
    try:
        root = ET.fromstring(body)
        for element in root.iter():
            element.tag = element.tag.rsplit("}", 1)[-1]
        if root.tag == "Acknowledgement_MarketDocument":
            raise EnergySourceError(
                "ENTSO-E acknowledgement: no usable data; check request/access."
            )
        if root.findtext("docStatus/value") in {"A09", "A13"}:
            raise EnergySourceError("Cancelled/withdrawn documents need operator investigation.")

        def required(node: ET.Element, path: str) -> str:
            value = node.findtext(path)
            if value is None or not value.strip():
                raise EnergySourceError(f"Missing required XML field: {path}")
            return value.strip()

        if kind not in {"generation", "price", "load_forecast"}:
            raise EnergySourceError("Unsupported energy metric.")
        expected = {"generation": "A75", "price": "A44", "load_forecast": "A65"}[kind]
        if required(root, "type") != expected:
            raise EnergySourceError(f"Expected {expected} document.")
        if kind == "generation" and required(root, "process.processType") != "A16":
            raise EnergySourceError("Only realised generation (A16) is accepted.")
        if kind == "load_forecast" and required(root, "process.processType") != "A01":
            raise EnergySourceError("A65 requires day-ahead forecast process A01, not actual load.")
        document = required(root, "mRID")
        revision = int(required(root, "revisionNumber"))
        # XML document creation time can be response-generated, not forecast publication.
        published = strict_utc(required(root, "createdDateTime"))
        points: list[EnergyPoint] = []
        for series in root.findall("TimeSeries"):
            incoming = series.findtext("inBiddingZone_Domain.mRID")
            outgoing = series.findtext("outBiddingZone_Domain.mRID")
            if kind == "generation":
                if outgoing == AUSTRIA and incoming is None:
                    # Pumped-storage consumption is not generation.
                    continue
                if incoming != AUSTRIA or outgoing is not None:
                    raise EnergySourceError("Generation series is not Austrian aggregate output.")
                psr = required(series, "MktPSRType/psrType")
                unit = required(series, "quantity_Measure_Unit.name")
            elif kind == "load_forecast":
                if required(series, "outBiddingZone_Domain.mRID") != AUSTRIA or incoming:
                    raise EnergySourceError(
                        "Load forecast must describe the Austrian bidding zone."
                    )
                if required(series, "businessType") != "A04":
                    raise EnergySourceError("Load forecast must describe consumption (A04).")
                if series.find("MktPSRType") is not None:
                    raise EnergySourceError("A total load forecast is not generation per PSR type.")
                unit = required(series, "quantity_Measure_Unit.name")
                psr = "load"
            else:
                if incoming != AUSTRIA or outgoing != AUSTRIA:
                    raise EnergySourceError("Prices must use the Austrian bidding zone.")
                if required(series, "contract_MarketAgreement.type") != "A01":
                    raise EnergySourceError("Only day-ahead (A01) prices are accepted.")
                if (
                    required(series, "currency_Unit.name") != "EUR"
                    or required(series, "price_Measure_Unit.name") != "MWH"
                ):
                    raise EnergySourceError("Price units must be EUR/MWh.")
                psr, unit = "price", "EUR/MWh"
            curve = required(series, "curveType")
            if curve not in {"A01", "A03"}:
                raise EnergySourceError("Unsupported ENTSO-E curve type.")
            for period in series.findall("Period"):
                period_start = strict_utc(required(period, "timeInterval/start"))
                period_end = strict_utc(required(period, "timeInterval/end"))
                resolution = required(period, "resolution")
                if resolution not in {"PT15M", "PT30M", "PT60M"}:
                    raise EnergySourceError("Unsupported interval resolution.")
                step = timedelta(minutes=int(re.fullmatch(r"PT(\d+)M", resolution)[1]))
                slots = (period_end - period_start) / step
                if not slots.is_integer() or not 0 < slots <= 200:
                    raise EnergySourceError("Invalid or oversized XML period.")
                values: dict[int, float | None] = {}
                positions = []
                for point in period.findall("Point"):
                    position = int(required(point, "position"))
                    if not 1 <= position <= slots or position in values:
                        raise EnergySourceError("Duplicate/out-of-range point position.")
                    raw = point.findtext("price.amount" if kind == "price" else "quantity")
                    values[position] = float(raw) if raw and raw.strip() else None
                    positions.append(position)
                if positions != sorted(positions):
                    raise EnergySourceError("Provider point positions are not ordered.")
                for index, position in enumerate(positions):
                    last = (
                        (positions[index + 1] if index + 1 < len(positions) else int(slots) + 1)
                        if curve == "A03"
                        else position + 1
                    )
                    for slot in range(position, last):
                        event = period_start + (slot - 1) * step
                        if start <= event and event + step <= end:
                            points.append(
                                EnergyPoint(
                                    event,
                                    event + step,
                                    kind,
                                    psr,
                                    values[position],
                                    unit,
                                    document,
                                    revision,
                                    published,
                                )
                            )
        validate_points(points)
        return points
    except (ET.ParseError, ValueError, TypeError) as error:
        if isinstance(error, EnergySourceError):
            raise
        raise EnergySourceError(f"Invalid ENTSO-E XML: {type(error).__name__}.") from None


CSV_COLUMNS = {
    "interval_start",
    "interval_end",
    "metric",
    "production_type",
    "value",
    "unit",
    "document_id",
    "revision",
    "published_at",
    "region",
}


def parse_csv(body: bytes, kind: Kind, start: datetime, end: datetime) -> list[EnergyPoint]:
    """Explicit normalized import contract, NOT a guessed native APG/File Library schema."""
    if len(body) > 20_000_000:
        raise EnergySourceError("CSV exceeds 20 MB safety limit.")
    try:
        reader = csv.DictReader(io.StringIO(body.decode("utf-8-sig")))
        if set(reader.fieldnames or []) != CSV_COLUMNS:
            raise EnergySourceError("CSV headers must match the documented normalized contract.")
        points = []
        for row in reader:
            if row["region"] != AUSTRIA or row["metric"] != kind:
                raise EnergySourceError("CSV region/metric does not match requested Austria data.")
            point = EnergyPoint(
                strict_utc(row["interval_start"]),
                strict_utc(row["interval_end"]),
                kind,
                row["production_type"],
                float(row["value"]) if row["value"] else None,
                row["unit"],
                row["document_id"],
                int(row["revision"]),
                strict_utc(row["published_at"]),
            )
            if not start <= point.start < point.end <= end:
                raise EnergySourceError("CSV interval outside the explicit import window.")
            points.append(point)
        validate_points(points)
        return points
    except (UnicodeError, ValueError, TypeError, KeyError, csv.Error) as error:
        if isinstance(error, EnergySourceError):
            raise
        raise EnergySourceError(f"Invalid normalized CSV: {type(error).__name__}.") from None

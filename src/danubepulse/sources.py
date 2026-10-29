"""Documented live-source connectors with bounded retries and explicit provenance."""

import json
import logging
import math
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from hashlib import sha256
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

from danubepulse.time_utils import parse_utc, utc_now

logger = logging.getLogger(__name__)

HYDRO_FEATURES_ENDPOINT = (
    "https://gis.lfrz.gv.at/api/geodata/i000501/ogc/features/v1/"
    "collections/i000501:pegel_aktuell/items"
)
HYDRO_COLLECTION_URL = (
    "https://gis.lfrz.gv.at/api/geodata/i000501/ogc/features/v1/collections/i000501:pegel_aktuell"
)
OPEN_METEO_ARCHIVE_ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"

DEFAULT_DANUBE_STATIONS: dict[int, str] = {
    207019: "Achleiten",
    207068: "Linz",
    207357: "Kienstock",
}
OPEN_METEO_VARIABLES = (
    "temperature_2m",
    "precipitation",
    "relative_humidity_2m",
    "wind_speed_10m",
)
OPEN_METEO_VARIABLE_FIELDS = {
    "temperature_2m": ("temperature_c", "°C"),
    "precipitation": ("precipitation_mm", "mm"),
    "relative_humidity_2m": ("relative_humidity_percent", "%"),
    "wind_speed_10m": ("wind_speed_m_s", "m/s"),
}
HttpJsonGet = Callable[[str], dict[str, Any]]


class SourceUnavailable(RuntimeError):
    """Raised when a documented source cannot return a valid response."""


@dataclass(frozen=True)
class SourceBatch:
    source_id: str
    retrieved_at: datetime
    published_at: datetime | None
    records: list[dict[str, Any]]
    provenance: str
    checksum: str
    raw_response: dict[str, Any] = field(default_factory=dict)
    stations: list[dict[str, Any]] = field(default_factory=list)
    rejected_records: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[dict[str, Any]] = field(default_factory=list)
    request_parameters: dict[str, Any] = field(default_factory=dict)


class SourceConnector:
    """Base class for small, injectable JSON API connectors."""

    def __init__(self, http_get: HttpJsonGet | None = None) -> None:
        self._http_get = http_get or _get_json


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _get_json(url: str) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            request = Request(
                url, headers={"Accept": "application/json", "User-Agent": "DanubePulse/0.1"}
            )
            with urlopen(request, timeout=20) as response:
                payload = json.loads(response.read())
            if not isinstance(payload, dict):
                raise SourceUnavailable("Provider returned JSON with an unexpected top-level type.")
            return payload
        except HTTPError as error:
            last_error = error
            if error.code < 500 and error.code != 429:
                raise SourceUnavailable(
                    f"Provider returned HTTP {error.code}: {error.reason}"
                ) from error
        except (URLError, TimeoutError, json.JSONDecodeError) as error:
            last_error = error
        if attempt < 2:
            delay = 0.5 * (2**attempt)
            logger.warning("source_request_retry", extra={"attempt": attempt + 1, "delay_s": delay})
            time.sleep(delay)
    raise SourceUnavailable(
        f"Provider request failed after three attempts: {last_error}"
    ) from last_error


class AustrianHydrographyConnector(SourceConnector):
    """Fetch current Austrian gauges and normalize selected Danube W/Q observations."""

    source_id = "ehyd-ogc-api"
    provenance = "real"

    def __init__(
        self,
        station_ids: tuple[int, ...] = tuple(DEFAULT_DANUBE_STATIONS),
        http_get: HttpJsonGet | None = None,
    ) -> None:
        super().__init__(http_get)
        self.station_ids = station_ids

    def fetch(self) -> SourceBatch:
        next_url: str | None = (
            HYDRO_FEATURES_ENDPOINT + "?" + urlencode({"limit": 500, "f": "json"})
        )
        pages: list[dict[str, Any]] = []
        while next_url:
            page = self._http_get(next_url)
            if page.get("type") != "FeatureCollection" or not isinstance(
                page.get("features"), list
            ):
                raise SourceUnavailable("eHYD OGC API returned an invalid FeatureCollection.")
            links = page.get("links", [])
            if not isinstance(links, list):
                raise SourceUnavailable("eHYD response links are not a JSON list.")
            pages.append(page)
            next_link = next(
                (
                    link.get("href")
                    for link in links
                    if isinstance(link, dict) and link.get("rel") == "next"
                ),
                None,
            )
            if next_link is not None and not isinstance(next_link, str):
                raise SourceUnavailable("eHYD pagination link is not a URL string.")
            next_url = urljoin(next_url, next_link) if next_link else None
            api_root = HYDRO_FEATURES_ENDPOINT.split("/items")[0]
            if next_url and not next_url.startswith(api_root):
                raise SourceUnavailable("eHYD pagination link points outside the documented API.")
            if len(pages) > 10:
                raise SourceUnavailable("eHYD pagination exceeded the safety limit.")

        retrieved_at = utc_now()
        records: list[dict[str, Any]] = []
        stations: dict[str, dict[str, Any]] = {}
        rejected: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []
        seen_stations: set[int] = set()
        expected_properties = {
            "hzbnr",
            "messstelle",
            "wert",
            "einheit",
            "zeitpunkt",
            "hydrodienst",
            "gewaesser",
            "parameter",
            "gesamtcode",
            "internet",
        }
        for page in pages:
            links = page.get("links", [])
            if not isinstance(links, list):
                raise SourceUnavailable("eHYD response links are not a JSON list.")
            for feature in page["features"]:
                if not isinstance(feature, dict):
                    warnings.append({"feature_id": None, "reason": "Feature is not a JSON object."})
                    continue
                properties = feature.get("properties")
                if not isinstance(properties, dict):
                    warnings.append(
                        {
                            "feature_id": feature.get("id"),
                            "reason": "Feature properties are missing or not an object.",
                        }
                    )
                    continue
                unexpected = sorted(set(properties) - expected_properties)
                if unexpected:
                    warnings.append(
                        {
                            "feature_id": feature.get("id"),
                            "reason": f"Unexpected source fields: {', '.join(unexpected)}.",
                        }
                    )
                try:
                    hydro_id = int(properties["hzbnr"])
                except (KeyError, TypeError, ValueError):
                    continue
                if hydro_id not in self.station_ids:
                    continue
                seen_stations.add(hydro_id)
                station_id = f"ehyd:{hydro_id}"
                geometry = feature.get("geometry")
                coordinates = geometry.get("coordinates") if isinstance(geometry, dict) else None
                name = properties.get("messstelle")
                water_body = properties.get("gewaesser")
                parameter = str(properties.get("parameter", "")).strip().upper()
                unit = str(properties.get("einheit", "")).strip()
                value = properties.get("wert")
                event_time_raw = properties.get("zeitpunkt")
                if (
                    not isinstance(name, str)
                    or not name.strip()
                    or not isinstance(coordinates, list)
                    or len(coordinates) < 2
                    or not isinstance(value, (int, float))
                    or isinstance(value, bool)
                    or not math.isfinite(float(value))
                    or not isinstance(event_time_raw, str)
                ):
                    rejected.append(
                        {
                            "source_record_id": feature.get("id", station_id),
                            "reason": (
                                "Required station, coordinate, value, or timestamp "
                                "field is invalid."
                            ),
                        }
                    )
                    continue
                try:
                    event_time = parse_utc(event_time_raw)
                    longitude, latitude = float(coordinates[0]), float(coordinates[1])
                    if not (-180 <= longitude <= 180 and -90 <= latitude <= 90):
                        raise ValueError("coordinates outside geographic range")
                except (TypeError, ValueError):
                    rejected.append(
                        {
                            "source_record_id": feature.get("id", station_id),
                            "reason": "Timestamp or geographic coordinates are invalid.",
                        }
                    )
                    continue

                level_m: float | None = None
                discharge_m3_s: float | None = None
                reference: str | None = None
                normalized_unit: str
                if parameter == "W" and unit.casefold() in {"cm", "m"}:
                    level_m = float(value) / 100 if unit.casefold() == "cm" else float(value)
                    reference = "gauge_reference_unspecified"
                    normalized_unit = "m"
                    measurement_type = "water_level"
                elif parameter == "Q" and unit.casefold().replace("³", "3") in {
                    "m3/s",
                    "m^3/s",
                }:
                    discharge_m3_s = float(value)
                    normalized_unit = "m³/s"
                    measurement_type = "discharge"
                else:
                    rejected.append(
                        {
                            "source_record_id": feature.get("id", station_id),
                            "reason": (
                                f"Unsupported parameter/unit combination: {parameter}/{unit}."
                            ),
                        }
                    )
                    continue
                stations[station_id] = {
                    "id": station_id,
                    "source_id": self.source_id,
                    "name": name.strip(),
                    "kind": "river",
                    "latitude": latitude,
                    "longitude": longitude,
                    "official_station_id": hydro_id,
                    "water_body": water_body,
                    "hydro_service": properties.get("hydrodienst"),
                    "station_url": properties.get("internet"),
                }
                record_id = f"ehyd:{hydro_id}:{parameter}:{event_time.isoformat()}"
                records.append(
                    {
                        "record_id": record_id,
                        "station_id": station_id,
                        "event_time": event_time.isoformat(),
                        "measurement_type": measurement_type,
                        "source_value": float(value),
                        "source_unit": unit,
                        "normalized_unit": normalized_unit,
                        "measurement_reference": reference,
                        "water_level_m": level_m,
                        "discharge_m3_s": discharge_m3_s,
                        "parameter": parameter,
                        "station_name": name.strip(),
                    }
                )

        missing_stations = sorted(set(self.station_ids) - seen_stations)
        raw_response = {"pages": pages}
        checksum = sha256(_canonical_json(raw_response)).hexdigest()
        return SourceBatch(
            source_id=self.source_id,
            retrieved_at=retrieved_at,
            published_at=None,
            records=records,
            provenance=self.provenance,
            checksum=checksum,
            raw_response=raw_response,
            stations=list(stations.values()),
            rejected_records=rejected,
            warnings=warnings,
            request_parameters={
                "collection": HYDRO_COLLECTION_URL,
                "requested_hzbnr": list(self.station_ids),
                "missing_hzbnr": missing_stations,
                "response_format": "application/json",
                "measurement_scope": "current gauge readings; no historical series",
            },
        )


class OpenMeteoArchiveConnector(SourceConnector):
    """Fetch hourly, model-based reanalysis for an explicit UTC date range."""

    source_id = "open-meteo-archive"
    provenance = "real"
    endpoint = OPEN_METEO_ARCHIVE_ENDPOINT

    def __init__(
        self,
        latitude: float,
        longitude: float,
        station_id: str,
        station_name: str,
        start_date: str,
        end_date: str,
        variables: tuple[str, ...] = OPEN_METEO_VARIABLES,
        models: str | None = None,
        http_get: HttpJsonGet | None = None,
    ) -> None:
        super().__init__(http_get)
        self.latitude = latitude
        self.longitude = longitude
        self.station_id = station_id
        self.station_name = station_name
        self.start_date = date.fromisoformat(start_date)
        self.end_date = date.fromisoformat(end_date)
        if self.end_date < self.start_date:
            raise ValueError("Weather end_date must not be earlier than start_date.")
        if (self.end_date - self.start_date).days > 30:
            raise ValueError("A single weather backfill request is limited to 31 days.")
        unknown = set(variables) - OPEN_METEO_VARIABLE_FIELDS.keys()
        if not variables or unknown:
            raise ValueError(f"Unsupported Open-Meteo variables: {sorted(unknown)}")
        if not all(math.isfinite(value) for value in (latitude, longitude)):
            raise ValueError("Weather coordinates must be finite numbers.")
        if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
            raise ValueError("Weather coordinates are outside the supported geographic range.")
        self.variables = variables
        self.models = models

    def fetch(self) -> SourceBatch:
        params: dict[str, str | float] = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "hourly": ",".join(self.variables),
            "timezone": "UTC",
            "wind_speed_unit": "ms",
        }
        if self.models:
            params["models"] = self.models
        url = f"{self.endpoint}?{urlencode(params)}"
        payload = self._http_get(url)
        hourly = payload.get("hourly")
        units = payload.get("hourly_units")
        if not isinstance(hourly, dict) or not isinstance(hourly.get("time"), list):
            raise SourceUnavailable("Open-Meteo response omitted its hourly time array.")
        if not isinstance(units, dict):
            raise SourceUnavailable("Open-Meteo response omitted hourly units.")
        times = hourly["time"]
        for variable in self.variables:
            if not isinstance(hourly.get(variable), list) or len(hourly[variable]) != len(times):
                raise SourceUnavailable(f"Open-Meteo returned inconsistent values for {variable}.")
            expected_unit = OPEN_METEO_VARIABLE_FIELDS[variable][1]
            actual_unit = str(units.get(variable, ""))
            supported_units = (
                {expected_unit, "km/h"} if variable == "wind_speed_10m" else {expected_unit}
            )
            if actual_unit not in supported_units:
                raise SourceUnavailable(
                    f"Unsupported {variable} unit {actual_unit!r}; expected {expected_unit!r}."
                )
        raw_response = {"request": params, "response": payload}
        records: list[dict[str, Any]] = []
        for index, time_value in enumerate(times):
            if not isinstance(time_value, str):
                raise SourceUnavailable("Open-Meteo returned a non-string hourly timestamp.")
            try:
                event_time = parse_utc(
                    time_value if time_value.endswith(("Z", "+00:00")) else f"{time_value}+00:00"
                )
            except ValueError as error:
                raise SourceUnavailable(
                    f"Open-Meteo returned an invalid timestamp: {time_value}"
                ) from error
            record: dict[str, Any] = {
                "record_id": f"{self.station_id}:{event_time.isoformat()}",
                "station_id": self.station_id,
                "event_time": event_time.isoformat(),
                "source_model": str(
                    payload.get("model")
                    or payload.get("models")
                    or self.models
                    or "provider-selected"
                ),
                "source_units": {name: str(units.get(name, "")) for name in self.variables},
            }
            for variable in self.variables:
                value = hourly[variable][index]
                if value is not None and (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(float(value))
                ):
                    raise SourceUnavailable(f"Open-Meteo returned a non-numeric {variable} value.")
                field_name, expected_unit = OPEN_METEO_VARIABLE_FIELDS[variable]
                source_unit = str(units.get(variable, ""))
                if variable == "wind_speed_10m" and source_unit == "km/h":
                    value = float(value) / 3.6 if value is not None else None
                elif value is not None and source_unit != expected_unit:
                    raise SourceUnavailable(
                        f"Unsupported {variable} unit {source_unit!r}; expected {expected_unit!r}."
                    )
                record[field_name] = float(value) if value is not None else None
            records.append(record)

        try:
            actual_latitude = float(payload.get("latitude", self.latitude))
            actual_longitude = float(payload.get("longitude", self.longitude))
        except (TypeError, ValueError) as error:
            raise SourceUnavailable("Open-Meteo returned invalid grid coordinates.") from error
        if not (
            math.isfinite(actual_latitude)
            and math.isfinite(actual_longitude)
            and -90 <= actual_latitude <= 90
            and -180 <= actual_longitude <= 180
        ):
            raise SourceUnavailable("Open-Meteo returned grid coordinates outside the globe.")
        station = {
            "id": self.station_id,
            "source_id": self.source_id,
            "name": self.station_name,
            "kind": "weather",
            "latitude": actual_latitude,
            "longitude": actual_longitude,
        }
        return SourceBatch(
            source_id=self.source_id,
            retrieved_at=utc_now(),
            published_at=None,
            records=records,
            provenance=self.provenance,
            checksum=sha256(_canonical_json(raw_response)).hexdigest(),
            raw_response=raw_response,
            stations=[station],
            request_parameters=params,
        )

"""Relational schema for source data, quality, incidents, and lineage."""

from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class DataSource(Base):
    __tablename__ = "data_sources"

    id: Mapped[str] = mapped_column(String(80), primary_key=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    provenance: Mapped[str] = mapped_column(String(40), nullable=False)
    license: Mapped[str | None] = mapped_column(String(160))
    reference_url: Mapped[str | None] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    runs: Mapped[list["IngestionRun"]] = relationship(back_populates="source")


class Station(Base):
    __tablename__ = "stations"

    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    source_id: Mapped[str] = mapped_column(ForeignKey("data_sources.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    kind: Mapped[str] = mapped_column(String(20), nullable=False)
    latitude: Mapped[float | None] = mapped_column(Float)
    longitude: Mapped[float | None] = mapped_column(Float)


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    source_id: Mapped[str] = mapped_column(ForeignKey("data_sources.id"), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    record_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    checksum: Mapped[str] = mapped_column(String(64), nullable=False)
    bronze_path: Mapped[str] = mapped_column(Text, nullable=False)
    source_published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    request_key: Mapped[str | None] = mapped_column(String(240), index=True)
    request_parameters: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    accepted_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    rejected_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    source: Mapped[DataSource] = relationship(back_populates="runs")


class RiverObservation(Base):
    __tablename__ = "river_observations"
    __table_args__ = (
        UniqueConstraint("source_id", "source_record_id", name="uq_river_source_record"),
        Index("ix_river_station_event", "station_id", "event_time"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[str] = mapped_column(ForeignKey("data_sources.id"), nullable=False)
    run_id: Mapped[str] = mapped_column(ForeignKey("ingestion_runs.id"), nullable=False)
    station_id: Mapped[str] = mapped_column(ForeignKey("stations.id"), nullable=False)
    source_record_id: Mapped[str] = mapped_column(String(120), nullable=False)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    water_level_m: Mapped[float | None] = mapped_column(Float)
    discharge_m3_s: Mapped[float | None] = mapped_column(Float)
    measurement_type: Mapped[str | None] = mapped_column(String(24))
    source_value: Mapped[float | None] = mapped_column(Float)
    source_unit: Mapped[str | None] = mapped_column(String(40))
    measurement_reference: Mapped[str | None] = mapped_column(String(80))
    quality_status: Mapped[str] = mapped_column(String(20), nullable=False)


class WeatherObservation(Base):
    __tablename__ = "weather_observations"
    __table_args__ = (
        UniqueConstraint("source_id", "source_record_id", name="uq_weather_source_record"),
        Index("ix_weather_station_event", "station_id", "event_time"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[str] = mapped_column(ForeignKey("data_sources.id"), nullable=False)
    run_id: Mapped[str] = mapped_column(ForeignKey("ingestion_runs.id"), nullable=False)
    station_id: Mapped[str] = mapped_column(ForeignKey("stations.id"), nullable=False)
    source_record_id: Mapped[str] = mapped_column(String(120), nullable=False)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    temperature_c: Mapped[float | None] = mapped_column(Float)
    precipitation_mm: Mapped[float | None] = mapped_column(Float)
    relative_humidity_percent: Mapped[float | None] = mapped_column(Float)
    wind_speed_m_s: Mapped[float | None] = mapped_column(Float)
    source_model: Mapped[str | None] = mapped_column(String(80))
    source_units: Mapped[dict[str, str] | None] = mapped_column(JSON)
    quality_status: Mapped[str] = mapped_column(String(20), nullable=False)


class EnergyObservation(Base):
    __tablename__ = "energy_observations"
    __table_args__ = (
        UniqueConstraint("source_id", "source_record_id", name="uq_energy_source_record"),
        Index("ix_energy_region_event", "region", "event_time"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[str] = mapped_column(ForeignKey("data_sources.id"), nullable=False)
    run_id: Mapped[str] = mapped_column(ForeignKey("ingestion_runs.id"), nullable=False)
    source_record_id: Mapped[str] = mapped_column(String(120), nullable=False)
    region: Mapped[str] = mapped_column(String(100), nullable=False)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    generation_mw: Mapped[float | None] = mapped_column(Float)
    load_forecast_mw: Mapped[float | None] = mapped_column(Float)
    price_eur_mwh: Mapped[float | None] = mapped_column(Float)
    interval_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    duration_hours: Mapped[float | None] = mapped_column(Float)
    metric: Mapped[str | None] = mapped_column(String(24))
    production_type: Mapped[str | None] = mapped_column(String(8))
    document_id: Mapped[str | None] = mapped_column(String(160))
    revision: Mapped[int | None] = mapped_column(Integer)
    source_unit: Mapped[str | None] = mapped_column(String(20))
    quality_status: Mapped[str] = mapped_column(String(20), nullable=False)


class QualityResult(Base):
    __tablename__ = "quality_results"
    __table_args__ = (Index("ix_quality_run", "run_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("ingestion_runs.id"), nullable=False)
    check_name: Mapped[str] = mapped_column(String(80), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    details: Mapped[str] = mapped_column(Text, nullable=False)
    affected_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    phase: Mapped[str | None] = mapped_column(String(20))
    incident_id: Mapped[str | None] = mapped_column(ForeignKey("incidents.id"))


class Incident(Base):
    __tablename__ = "incidents"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("ingestion_runs.id"), nullable=False)
    category: Mapped[str] = mapped_column(String(40), nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    title: Mapped[str] = mapped_column(String(180), nullable=False)
    details: Mapped[str] = mapped_column(Text, nullable=False)
    downstream_impact: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    scenario: Mapped[str | None] = mapped_column(String(40))
    affected_records: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON)
    before_metrics: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    after_metrics: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    replays: Mapped[list["ReplayHistory"]] = relationship(back_populates="incident")
    events: Mapped[list["IncidentEvent"]] = relationship(back_populates="incident")


class ReplayHistory(Base):
    __tablename__ = "replay_history"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    incident_id: Mapped[str] = mapped_column(ForeignKey("incidents.id"), nullable=False)
    replayed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    before_metrics: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    after_metrics: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    validation_results: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON)
    incident: Mapped[Incident] = relationship(back_populates="replays")


class QuarantinedRecord(Base):
    __tablename__ = "quarantined_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    incident_id: Mapped[str] = mapped_column(ForeignKey("incidents.id"), nullable=False)
    source_record_id: Mapped[str] = mapped_column(String(120), nullable=False)
    raw_record: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    quarantined_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class IncidentEvent(Base):
    __tablename__ = "incident_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    incident_id: Mapped[str] = mapped_column(ForeignKey("incidents.id"), nullable=False)
    event_type: Mapped[str] = mapped_column(String(40), nullable=False)
    from_status: Mapped[str | None] = mapped_column(String(20))
    to_status: Mapped[str | None] = mapped_column(String(20))
    details: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    incident: Mapped[Incident] = relationship(back_populates="events")


class DataLineage(Base):
    __tablename__ = "data_lineage"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("ingestion_runs.id"), nullable=False)
    source_record_id: Mapped[str] = mapped_column(String(120), nullable=False)
    target_table: Mapped[str] = mapped_column(String(80), nullable=False)
    target_record_id: Mapped[str] = mapped_column(String(120), nullable=False)


class PipelineExecution(Base):
    __tablename__ = "pipeline_executions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    domain: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    orchestrator_run_id: Mapped[str] = mapped_column(String(300), nullable=False)
    target_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ingestion_run_id: Mapped[str | None] = mapped_column(ForeignKey("ingestion_runs.id"))
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error: Mapped[str | None] = mapped_column(Text)
    verification: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON)


class StreamingOutbox(Base):
    __tablename__ = "streaming_outbox"
    __table_args__ = (Index("ix_streaming_outbox_due", "published_at", "next_attempt_at"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    topic: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_error: Mapped[str | None] = mapped_column(String(200))


class StreamingReceipt(Base):
    __tablename__ = "streaming_receipts"

    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    deliveries: Mapped[int] = mapped_column(Integer, nullable=False, default=1)


class StreamingDeadLetter(Base):
    __tablename__ = "streaming_dead_letters"
    __table_args__ = (
        UniqueConstraint("topic", "partition", "offset", name="uq_streaming_dead_position"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    topic: Mapped[str] = mapped_column(String(100), nullable=False)
    partition: Mapped[int] = mapped_column(Integer, nullable=False)
    offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    raw_base64: Mapped[str] = mapped_column(Text, nullable=False)
    error: Mapped[str] = mapped_column(String(200), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class StreamingWorker(Base):
    __tablename__ = "streaming_workers"

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    role: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    error: Mapped[str | None] = mapped_column(String(200))
    details: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

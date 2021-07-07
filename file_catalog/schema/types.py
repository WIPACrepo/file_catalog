"""Metadata schema type hints."""

from typing import Any, Dict, List, Optional

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict

Date = str
EventID = int


class Checksum(TypedDict, total=False):
    """Checksum dict."""

    sha512: str


class LocationEntry(TypedDict, total=False):
    """Location entry."""

    site: str
    path: str
    archive: bool


class SoftwareEntry(TypedDict, total=False):
    """Software entry."""

    name: str
    version: str
    date: Date


class EventsData(TypedDict):
    """Events info."""

    first_event: Optional[int]
    last_event: Optional[int]
    event_count: int
    status: str


class Run(TypedDict):
    """Run dict."""

    run_number: int
    subrun_number: int
    part_number: int
    start_datetime: Optional[Date]  # ISO date
    end_datetime: Optional[Date]  # ISO date
    first_event: Optional[EventID]
    last_event: Optional[EventID]
    event_count: int


class GapEntry(TypedDict):
    """Gap dict."""

    start_event_id: EventID
    stop_event_id: EventID
    delta_time: float
    start_date: Date
    stop_date: Date


class Event(TypedDict):
    """Event entry."""

    event_id: EventID
    datetime: Date


class OfflineProcessingMetadata(TypedDict, total=False):
    """Offline Processing Metadata."""

    dataset_id: int
    season: Optional[int]
    season_name: Optional[str]
    L2_gcd_file: str
    L2_snapshot_id: int
    L2_production_version: int
    L3_source_dataset_id: int
    working_group: str
    validation_validated: bool
    validation_date: Date
    validation_software: SoftwareEntry
    livetime: Optional[float]
    gaps: Optional[List[GapEntry]]
    first_event: Optional[Event]
    last_event: Optional[Event]


class IceProdMetadata(TypedDict, total=True):
    """IceProd Metadata."""

    dataset: int
    dataset_id: str
    job: Optional[int]
    job_id: Optional[str]
    task: Optional[str]
    task_id: Optional[str]
    config: str


class SimulationMetadata(TypedDict, total=False):
    """Simulation Metadata."""

    generator: str
    composition: str
    geometry: str
    GCD_file: str
    bulk_ice_model: str
    hole_ice_model: str
    photon_propagator: str
    DOMefficiency: float
    atmosphere: int
    n_events: int
    oversampling: int
    DOMoversize: int
    energy_min: float
    energy_max: float
    power_law_index: str
    cylinder_length: float
    cylinder_radius: float
    zenith_min: float
    zenith_max: float
    hadronic_interaction: str


class LongTermArchiveMetadata(TypedDict, total=False):
    """Long Term Archive Metadata."""

    date_archived: Date


# ideally, we could do some kind of introspection, but this requires universal TypedDict support
simulation_metadata_types = {
    "generator": str,
    "composition": str,
    "geometry": str,
    "GCD_file": str,
    "bulk_ice_model": str,
    "hole_ice_model": str,
    "photon_propagator": str,
    "DOMefficiency": float,
    "atmosphere": int,
    "n_events": int,
    "oversampling": int,
    "DOMoversize": int,
    "energy_min": float,
    "energy_max": float,
    "power_law_index": str,
    "cylinder_length": float,
    "cylinder_radius": float,
    "zenith_min": float,
    "zenith_max": float,
    "hadronic_interaction": str,
}


class Metadata(TypedDict, total=False):
    """The file-catalog metadata.

    https://docs.google.com/document/d/14SanUWiYEbgarElt0YXSn_2We-rwT-ePO5Fg7rrM9lw/view#heading=h.yq8ukujsb797
    """

    # Database Fields
    uuid: str
    _links: Dict[str, Dict[str, Any]]
    meta_modify_date: Date

    # Basic File:
    logical_name: str
    locations: List[LocationEntry]
    file_size: int
    checksum: Checksum
    create_date: Date

    # i3 File:
    data_type: Optional[str]
    processing_level: Optional[str]
    content_status: str

    # /data/exp/* i3 File:
    software: Optional[List[SoftwareEntry]]
    run: Run
    offline_processing_metadata: OfflineProcessingMetadata

    # /data/sim/* i3 File:
    iceprod: IceProdMetadata
    simulation: SimulationMetadata

    # Long Term Archive (LTA):
    lta: LongTermArchiveMetadata

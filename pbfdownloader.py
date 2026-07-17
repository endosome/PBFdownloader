# PBF downloader
#
# This script collects online vector tiles into MBtiles databases.
#
# The script is intended to run in the background and do the downloads with low request rates.
# The aim is to allow the download of large map areas while not overburdening the map delivery service.
# Mass downloads at high request rates may lead to services blocking the downloads.
# It is also "good practice" to keep download rates at a fair use level.
#
# Please observe the copyrights and usage policies of the map data sources!
#
# Map configuration is stored in a json file, with the following scheme:
# {
#     "FreelyChosenMapName": {
#         "DownloadURL": "https://{server}.mapsource.tld/path/to/pbf/tiles/{z}/{x}/{y}/tile.pbf?any=get&var=needed",  # {server} is placeholder for serverparts (load balancing), {x}, {y} and {z} are placeholders for tile coordinate
#         "BoundingBox": [min_lon, min_lat, max_lon, max_lat],
#         "ServerParts": ["server1", "server2", ...],  # may be empty, i.e. [""], if no {server} placeholder is present in DownloadURL
#         "MBtilesDB": "/path/to/your/MBtiles-file.mbtiles",
#         "Name": "Mapname in the MBtiles DB",
#         "min_z": 0,
#         "max_z": 14,
#         "RequestsPerSecond": 0.67,  # preferred target average request rate
#         "ReadSpacing": 1.5  # legacy fallback if RequestsPerSecond is not configured
#     },
#     "NextMap": {...},
#     ...
# }
#
# Written by Hauke 2025+
#
# License: CC BY-SA 4.0 Attribution-ShareAlike 4.0 International
# (https://creativecommons.org/licenses/by-sa/4.0/)
#
# For a detailed description and instructions visit:
# https://projects.webvoss.de/2025/09/27/fair-use-download-of-large-vector-maps/
#

import datetime
import gzip
import json
import math
import random
import shutil
import signal
import sqlite3
import subprocess
import time
from email.utils import parsedate_to_datetime
from pathlib import Path
from types import FrameType
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

import requests
import typer

######## CONFIG start ######

# Write to DB every X new tiles collected
WRITE_INTERVAL: int = 250

# Write a progress report every X seconds
PROGRESS_INTERVAL: int = 10

# Change 2: Randomize request intervals by this fraction to avoid perfectly
# periodic requests
REQUEST_JITTER_FRACTION: float = 0.20

# Change 1: Stop individual HTTP requests from hanging indefinitely
REQUEST_TIMEOUT: float = 30

# Change 3: Default pause if a 429 response has no usable Retry-After header
DEFAULT_RETRY_AFTER: float = 60

# Change 4: Ensure backoff retries are available even when only one server part
# is configured
MINIMUM_REQUEST_ATTEMPTS: int = 3

# Change 4: Limit exponential retry backoff to a reasonable maximum
MAXIMUM_RETRY_BACKOFF: float = 300

# File for storing the download state
PROCESS_STATE_FILE: Path = Path("./DownloadState.txt")

# User Agent for the web requests - some services block non-browser user agents
HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) "
        "Gecko/20100101 Firefox/136.0"
    )
}

######## CONFIG end ######

MapSourceConfig = Dict[str, Any]
MapConfig = Dict[str, MapSourceConfig]
TileRecord = Tuple[int, int, int, bytes]
TileBatch = Tuple[TileRecord, ...]


class Status(NamedTuple):
    source: int
    x: int
    y: int
    z: int
    total_tile_count: int


run: bool = True

######## INIT end #######


# From:
# https://medium.com/@ty2/how-to-calculate-number-of-tiles-in-a-bounding-box-for-openstreetmaps-4bf8c3b767ac
#
# And be aware of Y-axis deviation:
# https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md
#
# Tile number check:
# https://labs.mapbox.com/what-the-tile/
def degrees_to_tile_number(
    latitude_degrees: float,
    longitude_degrees: float,
    zoom: int,
) -> Tuple[int, int]:
    latitude_radians: float = math.radians(latitude_degrees)
    tile_count: float = 2.0**zoom

    x_tile: int = int((longitude_degrees + 180.0) / 360.0 * tile_count)

    y_tile: int = int(
        (1.0 - math.asinh(math.tan(latitude_radians)) / math.pi) * tile_count / 2.0
    )

    return x_tile, y_tile


def write_global_status(
    file: Path,
    source: int,
    x: int,
    y: int,
    z: int,
    total_tile_count: int,
) -> None:
    with file.open("w") as status_file:
        status_file.write(str(source) + "\n")
        status_file.write(str(z) + "\n")
        status_file.write(str(x) + "\n")
        status_file.write(str(y) + "\n")
        status_file.write(str(total_tile_count) + "\n")

    print("Updated Download State")


def read_global_status(file: Path) -> Status:
    with file.open("r") as status_file:
        source: int = int(status_file.readline())
        z: int = int(status_file.readline())
        x: int = int(status_file.readline())
        y: int = int(status_file.readline())
        total_tile_count: int = int(status_file.readline())

    return Status(
        source=source,
        x=x,
        y=y,
        z=z,
        total_tile_count=total_tile_count,
    )


def write_map_status(file: Path, full_loops: int) -> None:
    with file.open("w") as status_file:
        status_file.write(str(full_loops) + "\n")


def read_map_status(file: Path) -> int:
    with file.open("r") as status_file:
        full_loops: int = int(status_file.readline())

    return full_loops


def log(message: str) -> None:
    print(message)


def count_database_tiles(database_file: Path) -> int:
    with sqlite3.connect(database_file) as connection:
        result: Optional[Tuple[int]] = connection.execute(
            "SELECT COUNT(*) FROM tiles"
        ).fetchone()

    if result is None:
        return 0

    return result[0]


def write_to_database(
    database_file: Path,
    tile_list: TileBatch,
    total_tile_count: int,
) -> int:
    with sqlite3.connect(database_file) as database:
        before_result: Optional[Tuple[int]] = database.execute(
            "SELECT COUNT(*) FROM tiles"
        ).fetchone()

        before_count: int = before_result[0] if before_result is not None else 0

        database.executemany(
            """
            INSERT OR REPLACE INTO tiles
            (zoom_level, tile_column, tile_row, tile_data)
            VALUES (?, ?, ?, ?)
            """,
            tile_list,
        )

        after_result: Optional[Tuple[int]] = database.execute(
            "SELECT COUNT(*) FROM tiles"
        ).fetchone()

        after_count: int = after_result[0] if after_result is not None else 0

    unique_added: int = after_count - before_count
    overwritten: int = len(tile_list) - unique_added

    log(
        (
            f"Wrote {len(tile_list)} downloaded tiles. "
            f"New unique tiles: {unique_added}. "
            f"Overwritten tiles: {overwritten}. "
            f"Database total: {after_count}"
        )
    )

    return after_count


# Change 2: Enforce the configured request rate and add random timing jitter
def wait_for_request_slot(
    next_request_time: float,
    base_request_spacing: float,
) -> float:
    current_time: float = time.monotonic()

    if current_time < next_request_time:
        time.sleep(next_request_time - current_time)

    request_start_time: float = time.monotonic()

    jitter_multiplier: float = random.uniform(
        1.0 - REQUEST_JITTER_FRACTION,
        1.0 + REQUEST_JITTER_FRACTION,
    )

    return request_start_time + base_request_spacing * jitter_multiplier


# Change 3: Support both numeric and HTTP-date Retry-After header values
def parse_retry_after(retry_after_value: Optional[str]) -> float:
    if retry_after_value is None:
        return DEFAULT_RETRY_AFTER

    try:
        return max(float(retry_after_value), 0)
    except (TypeError, ValueError):
        pass

    try:
        retry_date: datetime.datetime = parsedate_to_datetime(retry_after_value)

        if retry_date.tzinfo is None:
            retry_date = retry_date.replace(tzinfo=datetime.timezone.utc)

        current_date: datetime.datetime = datetime.datetime.now(datetime.timezone.utc)

        return max(
            (retry_date - current_date).total_seconds(),
            0,
        )
    except (TypeError, ValueError, OverflowError):
        return DEFAULT_RETRY_AFTER


# Change 4: Increase the pause exponentially after repeated transient failures
def calculate_retry_backoff(
    base_request_spacing: float,
    retry_counter: int,
) -> float:
    backoff_seconds: float = base_request_spacing * (2 ** max(retry_counter - 1, 0))

    return min(backoff_seconds, MAXIMUM_RETRY_BACKOFF)


def handle_stop_signals(
    signum: int,
    frame: Optional[FrameType],
) -> None:
    global run
    run = False


######## Procedures end #########


def main(
    config: Path = typer.Option(
        Path("mapconfig.json"),
        "--config",
        "-c",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="Path to the map configuration JSON file.",
    ),
) -> None:
    global run

    # CLI: Load the selected configuration file, defaulting to mapconfig.json
    with config.open("r") as json_data:
        map_list: MapConfig = json.load(json_data)

    map_sources: List[str] = list(map_list.keys())

    server_part_number: int = 0
    session_tile_count: int = 0
    total_tile_count: int = 0
    source_counter: int = 0
    vector_tiles: TileBatch = ()

    run = True

    # Change 1: Reuse TCP/TLS connections instead of opening a new connection
    # for every tile
    request_session: requests.Session = requests.Session()
    request_session.headers.update(HEADERS)

    log("------ Start at " + str(datetime.datetime.now()) + " ------")

    signal.signal(signal.SIGINT, handle_stop_signals)
    signal.signal(signal.SIGTERM, handle_stop_signals)

    if PROCESS_STATE_FILE.exists():
        pickup_status: Status = read_global_status(PROCESS_STATE_FILE)

        log(
            "Pick up from S,Z,X,Y,T: "
            + str(map_sources[pickup_status.source])
            + " "
            + str(pickup_status.z)
            + " "
            + str(pickup_status.x)
            + " "
            + str(pickup_status.y)
            + " "
            + str(pickup_status.total_tile_count)
        )

        min_z: int = pickup_status.z
        total_tile_count = pickup_status.total_tile_count
        source_counter = pickup_status.source
        pickup_done: bool = pickup_status.x == 0
        map_run: bool = True
    else:
        pickup_done = True

    while run:
        source_config: MapSourceConfig = map_list[map_sources[source_counter]]

        download_url: str = source_config["DownloadURL"]
        server_parts: List[str] = source_config["ServerParts"]
        mbtiles_database: Path = Path(source_config["MBtilesDB"])
        map_name: str = source_config["Name"]
        max_z: int = source_config["max_z"]
        bounding_box: List[float] = source_config["BoundingBox"]
        min_z_default: int = source_config["min_z"]

        # Change 5: Prefer an explicit requests-per-second setting, with
        # ReadSpacing as a legacy fallback
        if "RequestsPerSecond" in source_config:
            requests_per_second: float = float(source_config["RequestsPerSecond"])

            if requests_per_second <= 0:
                raise ValueError("RequestsPerSecond must be greater than zero")

            read_spacing: float = 1.0 / requests_per_second
        else:
            read_spacing = float(source_config["ReadSpacing"])

            if read_spacing <= 0:
                raise ValueError("ReadSpacing must be greater than zero")

            requests_per_second = 1.0 / read_spacing

        map_status_file: Path = Path(".") / (
            map_sources[source_counter] + "_status.txt"
        )

        if map_status_file.exists():
            full_loops: int = read_map_status(map_status_file)
        else:
            full_loops = 0

        if pickup_done:
            min_z = min_z_default

        map_run = True

        number_of_server_parts: int = len(server_parts)

        # Change 4: Use at least three attempts so backoff also works with a
        # single server
        max_retries: int = max(
            number_of_server_parts,
            MINIMUM_REQUEST_ATTEMPTS,
        )

        # Change 5: Track the next permitted request start time for this map
        # source
        next_request_time: float = time.monotonic()

        log(
            (
                f"Request rate: {requests_per_second:.3f} requests/s "
                f"(base spacing {read_spacing:.3f}s, "
                f"jitter ±{REQUEST_JITTER_FRACTION * 100:.0f}%)"
            )
        )

        ### For Debugging
        # print(download_url)
        # print(server_parts)
        # print(mbtiles_database)
        # print(map_name)
        # print(max_z)
        # print(min_z_default)
        # print(min_z)
        # print(bounding_box)
        # print(read_spacing)
        # print(map_status_file)
        # print(number_of_server_parts)
        # print(max_retries)
        # print(full_loops)

        with sqlite3.connect(mbtiles_database) as output_database:
            output_database.execute("""
                CREATE TABLE IF NOT EXISTS metadata (
                    name TEXT NOT NULL PRIMARY KEY,
                    value TEXT
                )
                """)

            output_database.execute("""
                CREATE TABLE IF NOT EXISTS tiles (
                    zoom_level INTEGER NOT NULL,
                    tile_column INTEGER NOT NULL,
                    tile_row INTEGER NOT NULL,
                    tile_data BLOB NOT NULL,
                    PRIMARY KEY (
                        zoom_level,
                        tile_column,
                        tile_row
                    )
                )
                """)

            output_database.executemany(
                """
                INSERT OR REPLACE INTO metadata (name, value)
                VALUES (?, ?)
                """,
                (
                    ("name", map_name),
                    ("format", "pbf"),
                    ("crs", "EPSG:3857"),
                    ("minzoom", str(min_z_default)),
                    ("maxzoom", str(max_z)),
                    # MBTiles 1.3 specification requires bounds in this order:
                    # left, bottom, right, top
                    # which corresponds to:
                    # west longitude, south latitude, east longitude,
                    # north latitude
                    (
                        "bounds",
                        ",".join(str(value) for value in bounding_box),
                    ),
                ),
            )

        total_tiles_to_process: int = 0

        for progress_z in range(min_z, max_z + 1):
            progress_lower_left: Tuple[int, int] = degrees_to_tile_number(
                bounding_box[1],
                bounding_box[0],
                progress_z,
            )

            progress_upper_right: Tuple[int, int] = degrees_to_tile_number(
                bounding_box[3],
                bounding_box[2],
                progress_z,
            )

            progress_min_x: int = progress_lower_left[0]
            progress_max_x: int = progress_upper_right[0]
            progress_min_y: int = progress_upper_right[1]
            progress_max_y: int = progress_lower_left[1]

            progress_width: int = progress_max_x - progress_min_x + 1

            if not pickup_done and progress_z == pickup_status.z:
                total_tiles_to_process += (
                    progress_max_y - pickup_status.y
                ) * progress_width + (progress_max_x - pickup_status.x + 1)
            else:
                total_tiles_to_process += progress_width * (
                    progress_max_y - progress_min_y + 1
                )

        overall_tiles_processed: int = 0
        successful_tiles_this_map: int = 0
        missing_tiles_this_map: int = 0
        progress_start_time: float = time.monotonic()
        last_progress_time: float = progress_start_time

        log("Total tiles scheduled for this run: " + str(total_tiles_to_process))

        while map_run and run:
            log(
                "Now processing "
                + map_sources[source_counter]
                + " ("
                + str(mbtiles_database)
                + ")"
            )

            for z in range(min_z, max_z + 1):
                # Coordinates for Google-style URLs
                lower_left: Tuple[int, int] = degrees_to_tile_number(
                    bounding_box[1],
                    bounding_box[0],
                    z,  # min latitude  # min longitude
                )

                upper_right: Tuple[int, int] = degrees_to_tile_number(
                    bounding_box[3],
                    bounding_box[2],
                    z,  # max latitude  # max longitude
                )

                min_x: int = lower_left[0]
                max_x: int = upper_right[0]
                min_y: int = upper_right[1]
                max_y: int = lower_left[1]

                # Google-Scheme to Mapbox/TMS Y is:
                # Y_mapbox = 2^Z - 1 - Y_tms
                y_conversion: int = 2**z - 1

                number_of_tiles: int = (max_x - min_x + 1) * (max_y - min_y + 1)

                write_global_status(
                    PROCESS_STATE_FILE,
                    source_counter,
                    min_x,
                    min_y,
                    z,
                    total_tile_count,
                )

                level_pickup: bool = not pickup_done

                if pickup_done:
                    start_y: int = min_y
                    level_tiles_to_process: int = number_of_tiles
                else:
                    start_y = pickup_status.y
                    level_tiles_to_process = (max_y - pickup_status.y) * (
                        max_x - min_x + 1
                    ) + (max_x - pickup_status.x + 1)

                level_tiles_processed: int = 0

                log(
                    "Will download Level "
                    + str(z)
                    + " - number of tiles: "
                    + str(number_of_tiles)
                    + (
                        " - remaining this run: " + str(level_tiles_to_process)
                        if level_pickup
                        else ""
                    )
                )

                for y in range(start_y, max_y + 1):
                    if pickup_done:
                        start_x: int = min_x
                    else:
                        start_x = pickup_status.x
                        pickup_done = True

                    for x in range(start_x, max_x + 1):
                        retry_counter: int = 0
                        tile_downloaded_successfully: bool = False
                        tile_missing: bool = False

                        while retry_counter < max_retries:
                            server_part: str = server_parts[server_part_number]

                            server_part_number += 1

                            if server_part_number == number_of_server_parts:
                                server_part_number = 0

                            url: str = (
                                download_url.replace(
                                    "{server}",
                                    server_part,
                                )
                                .replace("{x}", str(x))
                                .replace("{y}", str(y))
                                .replace("{z}", str(z))
                            )

                            # Change 2: Apply rate limiting and jitter before
                            # every attempt, including retries
                            next_request_time = wait_for_request_slot(
                                next_request_time,
                                read_spacing,
                            )

                            try:
                                # Change 1: Use the shared session and a finite
                                # request timeout
                                tile_download: requests.Response = request_session.get(
                                    url,
                                    timeout=REQUEST_TIMEOUT,
                                )
                            except requests.RequestException as request_error:
                                retry_counter += 1

                                if retry_counter == max_retries:
                                    log(
                                        (
                                            "Error: Failed to download tile "
                                            "Z,X,Y "
                                            + str(z)
                                            + " "
                                            + str(x)
                                            + " "
                                            + str(y)
                                        )
                                    )

                                    log("Request error: " + str(request_error))

                                    log("URL:" + url)

                                    run = False
                                else:
                                    # Change 4: Back off exponentially after
                                    # transient request failures
                                    backoff_seconds: float = calculate_retry_backoff(
                                        read_spacing,
                                        retry_counter,
                                    )

                                    log(
                                        (
                                            "Request error for tile Z,X,Y "
                                            + str(z)
                                            + " "
                                            + str(x)
                                            + " "
                                            + str(y)
                                            + ". Retrying in "
                                            + f"{backoff_seconds:.1f}"
                                            + " seconds."
                                        )
                                    )

                                    time.sleep(backoff_seconds)

                                if not run:
                                    break

                                continue

                            if tile_download.status_code == 200:
                                tile_data: bytes = gzip.compress(tile_download.content)

                                vector_tiles += (
                                    (
                                        z,
                                        x,
                                        y_conversion - y,
                                        tile_data,
                                    ),
                                )

                                session_tile_count += 1
                                successful_tiles_this_map += 1
                                tile_downloaded_successfully = True

                                # Flag as done
                                retry_counter = max_retries

                                if len(vector_tiles) == WRITE_INTERVAL:
                                    total_tile_count = write_to_database(
                                        mbtiles_database,
                                        vector_tiles,
                                        total_tile_count,
                                    )

                                    vector_tiles = ()

                                    write_global_status(
                                        PROCESS_STATE_FILE,
                                        source_counter,
                                        x,
                                        y,
                                        z,
                                        total_tile_count,
                                    )

                            # Change 3: Respect server-provided Retry-After
                            # delays when rate limited
                            elif tile_download.status_code == 429:
                                retry_counter += 1

                                retry_after_seconds: float = parse_retry_after(
                                    tile_download.headers.get("Retry-After")
                                )

                                if retry_counter == max_retries:
                                    log(
                                        (
                                            "Error: Rate limit persisted for "
                                            "tile Z,X,Y "
                                            + str(z)
                                            + " "
                                            + str(x)
                                            + " "
                                            + str(y)
                                        )
                                    )

                                    log("Status:" + str(tile_download.status_code))

                                    log("URL:" + tile_download.url)

                                    log(
                                        "Response headers:" + str(tile_download.headers)
                                    )

                                    run = False
                                else:
                                    log(
                                        (
                                            "Rate limited for tile Z,X,Y "
                                            + str(z)
                                            + " "
                                            + str(x)
                                            + " "
                                            + str(y)
                                            + ". Retrying in "
                                            + (f"{retry_after_seconds:.1f}")
                                            + " seconds."
                                        )
                                    )

                                    time.sleep(retry_after_seconds)

                            elif tile_download.status_code == 404:
                                retry_counter += 1

                                if retry_counter == max_retries:
                                    tile_missing = True
                                    missing_tiles_this_map += 1

                                    log(
                                        (
                                            "Warning: Tile Z,X,Y "
                                            + str(z)
                                            + " "
                                            + str(x)
                                            + " "
                                            + str(y)
                                            + (" seems out of bounds " "(404)")
                                        )
                                    )

                            else:
                                retry_counter += 1

                                if retry_counter == max_retries:
                                    log(
                                        (
                                            "Error: Failed to download tile "
                                            "Z,X,Y "
                                            + str(z)
                                            + " "
                                            + str(x)
                                            + " "
                                            + str(y)
                                        )
                                    )

                                    log("Status:" + str(tile_download.status_code))

                                    log("URL:" + tile_download.url)

                                    log(
                                        "Request headers:"
                                        + str(tile_download.request.headers)
                                    )

                                    log(
                                        "Response headers:" + str(tile_download.headers)
                                    )

                                    # Currently no way to handle errors more
                                    # gracefully - just stop the program and
                                    # retry with next run.
                                    run = False
                                else:
                                    # Change 4: Back off exponentially before
                                    # retrying other HTTP errors
                                    backoff_seconds = calculate_retry_backoff(
                                        read_spacing,
                                        retry_counter,
                                    )

                                    log(
                                        (
                                            "HTTP "
                                            + str(tile_download.status_code)
                                            + " for tile Z,X,Y "
                                            + str(z)
                                            + " "
                                            + str(x)
                                            + " "
                                            + str(y)
                                            + ". Retrying in "
                                            + f"{backoff_seconds:.1f}"
                                            + " seconds."
                                        )
                                    )

                                    time.sleep(backoff_seconds)

                            if not run:
                                break

                        level_tiles_processed += 1
                        overall_tiles_processed += 1

                        current_time: float = time.monotonic()

                        if (
                            current_time - last_progress_time >= PROGRESS_INTERVAL
                            or level_tiles_processed == level_tiles_to_process
                            or not run
                        ):
                            elapsed_seconds: float = current_time - progress_start_time

                            if elapsed_seconds > 0:
                                processing_speed: float = (
                                    overall_tiles_processed / elapsed_seconds
                                )
                            else:
                                processing_speed = 0

                            remaining_tiles: int = max(
                                total_tiles_to_process - overall_tiles_processed,
                                0,
                            )

                            if processing_speed > 0:
                                eta_seconds: float = remaining_tiles / processing_speed

                                eta_string: str = str(
                                    datetime.timedelta(seconds=int(eta_seconds))
                                )
                            else:
                                eta_string = "unknown"

                            if level_tiles_to_process > 0:
                                level_percent: float = (
                                    level_tiles_processed / level_tiles_to_process * 100
                                )
                            else:
                                level_percent = 100

                            if total_tiles_to_process > 0:
                                overall_percent: float = (
                                    overall_tiles_processed
                                    / total_tiles_to_process
                                    * 100
                                )
                            else:
                                overall_percent = 100

                            log(
                                (
                                    f"{datetime.datetime.now():%H:%M:%S} | "
                                    f"z{z} "
                                    f"{level_tiles_processed}/"
                                    f"{level_tiles_to_process} "
                                    f"({level_percent:.1f}%) | "
                                    f"overall "
                                    f"{overall_tiles_processed}/"
                                    f"{total_tiles_to_process} "
                                    f"({overall_percent:.1f}%) | "
                                    f"downloaded "
                                    f"{successful_tiles_this_map} | "
                                    f"missing "
                                    f"{missing_tiles_this_map} | "
                                    f"{processing_speed:.2f} tiles/s | "
                                    f"ETA {eta_string}"
                                )
                            )

                            last_progress_time = current_time

                        if not run:
                            break

                    if not run:
                        break

                if not run:
                    break

            # On SIGTERM or SIGINT and after full loop save DB
            if len(vector_tiles) > 0:
                total_tile_count = write_to_database(
                    mbtiles_database,
                    vector_tiles,
                    total_tile_count,
                )

            if session_tile_count > 0:
                write_global_status(
                    PROCESS_STATE_FILE,
                    source_counter,
                    x,
                    y,
                    z,
                    total_tile_count,
                )

            vector_tiles = ()

            if run:
                full_loops += 1
                write_map_status(
                    map_status_file,
                    full_loops,
                )

                subprocess.run(
                    [
                        "mbtiles",
                        "validate",
                        "--agg-hash",
                        "update",
                        str(mbtiles_database),
                    ],
                    check=True,
                )

                mbtiles_copy: Path = mbtiles_database.with_name(
                    mbtiles_database.stem + str(full_loops) + mbtiles_database.suffix
                )

                shutil.copy(
                    mbtiles_database,
                    mbtiles_copy,
                )

                log(
                    (
                        "Whole area processed completely - copy created and "
                        "start next mapsource."
                    )
                )

                source_counter += 1
                total_tile_count = 0
                map_run = False
                pickup_done = True

                if source_counter >= len(map_sources):
                    if PROCESS_STATE_FILE.exists():
                        PROCESS_STATE_FILE.unlink()

                    run = False

    request_session.close()

    log(("Shutdown received or error occured - graceful exit " "successfull."))

    log(
        "------ Download ended at "
        + str(datetime.datetime.now())
        + " after getting "
        + str(session_tile_count)
        + " tiles. ------"
    )


if __name__ == "__main__":
    typer.run(main)

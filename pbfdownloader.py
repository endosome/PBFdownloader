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
# 	"FreelyChosenMapName": {
# 		"DownloadURL": "https://{server}.mapsource.tld/path/to/pbf/tiles/{z}/{x}/{y}/tile.pbf?any=get&var=needed",     # {server} is placeholder for serverparts (load balancing), {x}, {y} and {z} are placeholders for tile coordinate
# 		"BoundingBox": [min_lon, min_lat, max_lon, max_lat],
# 		"ServerParts": ["server1", "server2", ...],                                                         # may be empty, i.e. [""], if no {server} placeholder is present in DownloadURL
# 		"MBtilesDB": "/path/to/your/MBtiles-file.mbtiles",
# 		"Name": "Mapname in the MBtiles DB",
# 		"min_z": 0,
# 		"max_z": 14,
# 		"RequestsPerSecond": 0.67,                                                                          # preferred target average request rate
# 		"ReadSpacing": 1.5                                                                                  # legacy fallback if RequestsPerSecond is not configured
# 	},
#   "NextMap": {...},
#   ...
# }
#
# Written by Hauke 2025+
#
# License: CC BY-SA 4.0 Attribution-ShareAlike 4.0 International (https://creativecommons.org/licenses/by-sa/4.0/)
#
# For a detailed description and instructions visit: https://projects.webvoss.de/2025/09/27/fair-use-download-of-large-vector-maps/
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
from time import sleep
from types import FrameType
from typing import Any, NamedTuple, TypeAlias

import requests
import typer

######## CONFIG start ######

# Write to DB every X new tiles collected
WriteInterval: int = 250

# Write a progress report every X seconds
ProgressInterval: int = 10

# Change 2: Randomize request intervals by this fraction to avoid perfectly periodic requests
RequestJitterFraction: float = 0.20

# Change 1: Stop individual HTTP requests from hanging indefinitely
RequestTimeout: float = 30

# Change 3: Default pause if a 429 response has no usable Retry-After header
DefaultRetryAfter: float = 60

# Change 4: Ensure backoff retries are available even when only one server part is configured
MinimumRequestAttempts: int = 3

# Change 4: Limit exponential retry backoff to a reasonable maximum
MaximumRetryBackoff: float = 300

# Files for storing the status
ProcessStateFile: Path = Path("./DownloadState.txt")
LogfileName: Path = Path("./download.log")

# User Agent for the web requests - some services block non-browser user agents
headers: dict[str, str] = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0"
}

######## CONFIG end ######

MapSourceConfig: TypeAlias = dict[str, Any]
MapConfig: TypeAlias = dict[str, MapSourceConfig]
TileRecord: TypeAlias = tuple[int, int, int, bytes]
TileBatch: TypeAlias = tuple[TileRecord, ...]


class Status(NamedTuple):
    Source: int
    X: int
    Y: int
    Z: int
    TotalTileCount: int


Run: bool = True

######## INIT end #######


# From https://medium.com/@ty2/how-to-calculate-number-of-tiles-in-a-bounding-box-for-openstreetmaps-4bf8c3b767ac
# And be aware of Y-axis deviation: https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md
# Tile number check: https://labs.mapbox.com/what-the-tile/
def deg2num(lat_deg: float, lon_deg: float, zoom: int) -> tuple[int, int]:
    lat_rad: float = math.radians(lat_deg)
    n: float = 2.0**zoom
    xtile: int = int((lon_deg + 180.0) / 360.0 * n)
    ytile: int = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) * n / 2.0)
    return xtile, ytile


def WriteGlobalStatus(
    File: Path,
    Source: int,
    X: int,
    Y: int,
    Z: int,
    TotalTileCount: int,
) -> None:
    with File.open("w") as StatusFile:
        StatusFile.write(str(Source) + "\n")
        StatusFile.write(str(Z) + "\n")
        StatusFile.write(str(X) + "\n")
        StatusFile.write(str(Y) + "\n")
        StatusFile.write(str(TotalTileCount) + "\n")

    print("Updated Download State")


def ReadGlobalStatus(File: Path) -> Status:
    with File.open("r") as StatusFile:
        Source: int = int(StatusFile.readline())
        Z: int = int(StatusFile.readline())
        X: int = int(StatusFile.readline())
        Y: int = int(StatusFile.readline())
        TotalTileCount: int = int(StatusFile.readline())

    return Status(
        Source=Source,
        X=X,
        Y=Y,
        Z=Z,
        TotalTileCount=TotalTileCount,
    )


def WriteMapStatus(File: Path, FullLoops: int) -> None:
    with File.open("w") as StatusFile:
        StatusFile.write(str(FullLoops) + "\n")


def ReadMapStatus(File: Path) -> int:
    with File.open("r") as StatusFile:
        FullLoops: int = int(StatusFile.readline())

    return FullLoops


def Log(LogfileName: Path, Message: str) -> None:
    print(Message)

    with LogfileName.open("a") as Logfile:
        Logfile.write(Message + "\n")


def CountDatabaseTiles(database_file: Path) -> int:
    with sqlite3.connect(database_file) as connection:
        result: tuple[int] | None = connection.execute(
            "SELECT COUNT(*) FROM tiles"
        ).fetchone()

    if result is None:
        return 0

    return result[0]


def WriteToDB(
    DatabaseFile: Path,
    TileList: TileBatch,
    LogfileName: Path,
    TotalTileCount: int,
) -> int:
    with sqlite3.connect(DatabaseFile) as database:
        before_result: tuple[int] | None = database.execute(
            "SELECT COUNT(*) FROM tiles"
        ).fetchone()

        before_count: int = before_result[0] if before_result is not None else 0

        database.executemany(
            """
            INSERT OR REPLACE INTO tiles
            (zoom_level, tile_column, tile_row, tile_data)
            VALUES (?, ?, ?, ?)
            """,
            TileList,
        )

        after_result: tuple[int] | None = database.execute(
            "SELECT COUNT(*) FROM tiles"
        ).fetchone()

        after_count: int = after_result[0] if after_result is not None else 0

    unique_added: int = after_count - before_count
    overwritten: int = len(TileList) - unique_added

    Log(
        LogfileName,
        (
            f"Wrote {len(TileList)} downloaded tiles. "
            f"New unique tiles: {unique_added}. "
            f"Overwritten tiles: {overwritten}. "
            f"Database total: {after_count}"
        ),
    )

    return after_count


# Change 2: Enforce the configured request rate and add random timing jitter
def WaitForRequestSlot(
    NextRequestTime: float,
    BaseRequestSpacing: float,
) -> float:
    CurrentTime: float = time.monotonic()

    if CurrentTime < NextRequestTime:
        sleep(NextRequestTime - CurrentTime)

    RequestStartTime: float = time.monotonic()

    JitterMultiplier: float = random.uniform(
        1.0 - RequestJitterFraction,
        1.0 + RequestJitterFraction,
    )

    return RequestStartTime + BaseRequestSpacing * JitterMultiplier


# Change 3: Support both numeric and HTTP-date Retry-After header values
def ParseRetryAfter(RetryAfterValue: str | None) -> float:
    if RetryAfterValue is None:
        return DefaultRetryAfter

    try:
        return max(float(RetryAfterValue), 0)
    except (TypeError, ValueError):
        pass

    try:
        RetryDate: datetime.datetime = parsedate_to_datetime(RetryAfterValue)

        if RetryDate.tzinfo is None:
            RetryDate = RetryDate.replace(tzinfo=datetime.timezone.utc)

        CurrentDate: datetime.datetime = datetime.datetime.now(datetime.timezone.utc)

        return max((RetryDate - CurrentDate).total_seconds(), 0)
    except (TypeError, ValueError, OverflowError):
        return DefaultRetryAfter


# Change 4: Increase the pause exponentially after repeated transient failures
def CalculateRetryBackoff(
    BaseRequestSpacing: float,
    RetryCounter: int,
) -> float:
    BackoffSeconds: float = BaseRequestSpacing * (2 ** max(RetryCounter - 1, 0))

    return min(BackoffSeconds, MaximumRetryBackoff)


def handler_stop_signals(
    signum: int,
    frame: FrameType | None,
) -> None:
    global Run
    Run = False


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
    global Run

    # CLI: Load the selected configuration file, defaulting to mapconfig.json
    with config.open("r") as json_data:
        Maplist: MapConfig = json.load(json_data)

    MapSources: list[str] = list(Maplist.keys())

    ServerPartNumber: int = 0
    SessionTileCount: int = 0
    TotalTileCount: int = 0
    SourceCounter: int = 0
    VectorTiles: TileBatch = ()

    Run = True

    # Change 1: Reuse TCP/TLS connections instead of opening a new connection for every tile
    RequestSession: requests.Session = requests.Session()
    RequestSession.headers.update(headers)

    Log(
        LogfileName,
        "------ Start at " + str(datetime.datetime.now()) + " ------",
    )

    signal.signal(signal.SIGINT, handler_stop_signals)
    signal.signal(signal.SIGTERM, handler_stop_signals)

    if ProcessStateFile.exists():
        PickupStatus: Status = ReadGlobalStatus(ProcessStateFile)

        Log(
            LogfileName,
            "Pick up from S,Z,X,Y,T: "
            + str(MapSources[PickupStatus.Source])
            + " "
            + str(PickupStatus.Z)
            + " "
            + str(PickupStatus.X)
            + " "
            + str(PickupStatus.Y)
            + " "
            + str(PickupStatus.TotalTileCount),
        )

        min_z: int = PickupStatus.Z
        TotalTileCount = PickupStatus.TotalTileCount
        SourceCounter = PickupStatus.Source
        PickupDone: bool = PickupStatus.X == 0
        MapRun: bool = True
    else:
        PickupDone = True

    while Run:
        SourceConfig: MapSourceConfig = Maplist[MapSources[SourceCounter]]

        DownloadURL: str = SourceConfig["DownloadURL"]
        ServerParts: list[str] = SourceConfig["ServerParts"]
        MBtilesDB: Path = Path(SourceConfig["MBtilesDB"])
        MapName: str = SourceConfig["Name"]
        max_z: int = SourceConfig["max_z"]
        BoundingBox: list[float] = SourceConfig["BoundingBox"]
        min_z0: int = SourceConfig["min_z"]

        # Change 5: Prefer an explicit requests-per-second setting, with ReadSpacing as a legacy fallback
        if "RequestsPerSecond" in SourceConfig:
            RequestsPerSecond: float = float(SourceConfig["RequestsPerSecond"])

            if RequestsPerSecond <= 0:
                raise ValueError("RequestsPerSecond must be greater than zero")

            ReadSpacing: float = 1.0 / RequestsPerSecond
        else:
            ReadSpacing = float(SourceConfig["ReadSpacing"])

            if ReadSpacing <= 0:
                raise ValueError("ReadSpacing must be greater than zero")

            RequestsPerSecond = 1.0 / ReadSpacing

        MapStatusFile: Path = Path(".") / (MapSources[SourceCounter] + "_status.txt")

        if MapStatusFile.exists():
            FullLoops: int = ReadMapStatus(MapStatusFile)
        else:
            FullLoops = 0

        if PickupDone:
            min_z = min_z0

        MapRun = True

        NumberOfServerParts: int = len(ServerParts)

        # Change 4: Use at least three attempts so backoff also works with a single server
        MaxRetries: int = max(
            NumberOfServerParts,
            MinimumRequestAttempts,
        )

        # Change 5: Track the next permitted request start time for this map source
        NextRequestTime: float = time.monotonic()

        Log(
            LogfileName,
            (
                f"Request rate: {RequestsPerSecond:.3f} requests/s "
                f"(base spacing {ReadSpacing:.3f}s, "
                f"jitter ±{RequestJitterFraction * 100:.0f}%)"
            ),
        )

        ### For Debugging
        # 	print(DownloadURL)
        # 	print(ServerParts)
        # 	print(MBtilesDB)
        # 	print(MapName)
        # 	print(max_z)
        # 	print(min_z0)
        # 	print(min_z)
        # 	print(BoundingBox)
        # 	print(ReadSpacing)
        # 	print(MapStatusFile)
        # 	print(NumberOfServerParts)
        # 	print(MaxRetries)
        # 	print(FullLoops)

        with sqlite3.connect(MBtilesDB) as out:
            out.execute("""
                CREATE TABLE IF NOT EXISTS metadata (
                    name TEXT NOT NULL PRIMARY KEY,
                    value TEXT
                )
                """)

            out.execute("""
                CREATE TABLE IF NOT EXISTS tiles (
                    zoom_level INTEGER NOT NULL,
                    tile_column INTEGER NOT NULL,
                    tile_row INTEGER NOT NULL,
                    tile_data BLOB NOT NULL,
                    PRIMARY KEY (zoom_level, tile_column, tile_row)
                )
                """)

            out.executemany(
                """
                INSERT OR REPLACE INTO metadata (name, value)
                VALUES (?, ?)
                """,
                (
                    ("name", MapName),
                    ("format", "pbf"),
                    ("crs", "EPSG:3857"),
                    ("minzoom", str(min_z0)),
                    ("maxzoom", str(max_z)),
                    # MBTiles 1.3 specification requires bounds in this order:
                    # left, bottom, right, top
                    # which corresponds to:
                    # west longitude, south latitude, east longitude, north latitude
                    ("bounds", ",".join(str(value) for value in BoundingBox)),
                ),
            )

        TotalTilesToProcess: int = 0

        for ProgressZ in range(min_z, max_z + 1):
            ProgressLowerLeft: tuple[int, int] = deg2num(
                BoundingBox[1],
                BoundingBox[0],
                ProgressZ,
            )

            ProgressUpperRight: tuple[int, int] = deg2num(
                BoundingBox[3],
                BoundingBox[2],
                ProgressZ,
            )

            ProgressMinX: int = ProgressLowerLeft[0]
            ProgressMaxX: int = ProgressUpperRight[0]
            ProgressMinY: int = ProgressUpperRight[1]
            ProgressMaxY: int = ProgressLowerLeft[1]

            ProgressWidth: int = ProgressMaxX - ProgressMinX + 1

            if not PickupDone and ProgressZ == PickupStatus.Z:
                TotalTilesToProcess += (
                    ProgressMaxY - PickupStatus.Y
                ) * ProgressWidth + (ProgressMaxX - PickupStatus.X + 1)
            else:
                TotalTilesToProcess += ProgressWidth * (ProgressMaxY - ProgressMinY + 1)

        OverallTilesProcessed: int = 0
        SuccessfulTilesThisMap: int = 0
        MissingTilesThisMap: int = 0
        ProgressStartTime: float = time.monotonic()
        LastProgressTime: float = ProgressStartTime

        Log(
            LogfileName,
            "Total tiles scheduled for this run: " + str(TotalTilesToProcess),
        )

        while MapRun and Run:
            Log(
                LogfileName,
                "Now processing "
                + MapSources[SourceCounter]
                + " ("
                + str(MBtilesDB)
                + ")",
            )

            for Z in range(min_z, max_z + 1):
                # Coordinates for Google-style URLs
                LowerLeft: tuple[int, int] = deg2num(
                    BoundingBox[1],
                    BoundingBox[0],
                    Z,  # min latitude  # min longitude
                )

                UpperRight: tuple[int, int] = deg2num(
                    BoundingBox[3],
                    BoundingBox[2],
                    Z,  # max latitude  # max longitude
                )

                min_x: int = LowerLeft[0]
                max_x: int = UpperRight[0]
                min_y: int = UpperRight[1]
                max_y: int = LowerLeft[1]

                # Google-Scheme to Mapbox/TMS Y is: Y_mapbox = 2^Z - 1 - Y_tms
                Yconversion: int = 2**Z - 1

                NumberOfTiles: int = (max_x - min_x + 1) * (max_y - min_y + 1)

                WriteGlobalStatus(
                    ProcessStateFile,
                    SourceCounter,
                    min_x,
                    min_y,
                    Z,
                    TotalTileCount,
                )

                LevelPickup: bool = not PickupDone

                if PickupDone:
                    StartY: int = min_y
                    LevelTilesToProcess: int = NumberOfTiles
                else:
                    StartY = PickupStatus.Y
                    LevelTilesToProcess = (max_y - PickupStatus.Y) * (
                        max_x - min_x + 1
                    ) + (max_x - PickupStatus.X + 1)

                LevelTilesProcessed: int = 0

                Log(
                    LogfileName,
                    "Will download Level "
                    + str(Z)
                    + " - number of tiles: "
                    + str(NumberOfTiles)
                    + (
                        " - remaining this run: " + str(LevelTilesToProcess)
                        if LevelPickup
                        else ""
                    ),
                )

                for Y in range(StartY, max_y + 1):
                    if PickupDone:
                        StartX: int = min_x
                    else:
                        StartX = PickupStatus.X
                        PickupDone = True

                    for X in range(StartX, max_x + 1):
                        RetryCounter: int = 0
                        TileDownloadedSuccessfully: bool = False
                        TileMissing: bool = False

                        while RetryCounter < MaxRetries:
                            ServerPart: str = ServerParts[ServerPartNumber]
                            ServerPartNumber += 1

                            if ServerPartNumber == NumberOfServerParts:
                                ServerPartNumber = 0

                            URL: str = (
                                DownloadURL.replace(
                                    "{server}",
                                    ServerPart,
                                )
                                .replace("{x}", str(X))
                                .replace("{y}", str(Y))
                                .replace("{z}", str(Z))
                            )

                            # Change 2: Apply rate limiting and jitter before every attempt, including retries
                            NextRequestTime = WaitForRequestSlot(
                                NextRequestTime,
                                ReadSpacing,
                            )

                            try:
                                # Change 1: Use the shared session and a finite request timeout
                                TileDownload: requests.Response = RequestSession.get(
                                    URL,
                                    timeout=RequestTimeout,
                                )
                            except requests.RequestException as RequestError:
                                RetryCounter += 1

                                if RetryCounter == MaxRetries:
                                    Log(
                                        LogfileName,
                                        "Error: Failed to download tile Z,X,Y "
                                        + str(Z)
                                        + " "
                                        + str(X)
                                        + " "
                                        + str(Y),
                                    )

                                    Log(
                                        LogfileName,
                                        "Request error: " + str(RequestError),
                                    )

                                    Log(LogfileName, "URL:" + URL)
                                    Run = False
                                else:
                                    # Change 4: Back off exponentially after transient request failures
                                    BackoffSeconds: float = CalculateRetryBackoff(
                                        ReadSpacing,
                                        RetryCounter,
                                    )

                                    Log(
                                        LogfileName,
                                        (
                                            "Request error for tile Z,X,Y "
                                            + str(Z)
                                            + " "
                                            + str(X)
                                            + " "
                                            + str(Y)
                                            + ". Retrying in "
                                            + f"{BackoffSeconds:.1f}"
                                            + " seconds."
                                        ),
                                    )

                                    sleep(BackoffSeconds)

                                if not Run:
                                    break

                                continue

                            if TileDownload.status_code == 200:
                                TileData: bytes = gzip.compress(TileDownload.content)

                                VectorTiles += (
                                    (
                                        Z,
                                        X,
                                        Yconversion - Y,
                                        TileData,
                                    ),
                                )

                                SessionTileCount += 1
                                SuccessfulTilesThisMap += 1
                                TileDownloadedSuccessfully = True

                                RetryCounter = MaxRetries  # Flag as done

                                if len(VectorTiles) == WriteInterval:
                                    TotalTileCount = WriteToDB(
                                        MBtilesDB,
                                        VectorTiles,
                                        LogfileName,
                                        TotalTileCount,
                                    )

                                    VectorTiles = ()

                                    WriteGlobalStatus(
                                        ProcessStateFile,
                                        SourceCounter,
                                        X,
                                        Y,
                                        Z,
                                        TotalTileCount,
                                    )

                            # Change 3: Respect server-provided Retry-After delays when rate limited
                            elif TileDownload.status_code == 429:
                                RetryCounter += 1

                                RetryAfterSeconds: float = ParseRetryAfter(
                                    TileDownload.headers.get("Retry-After")
                                )

                                if RetryCounter == MaxRetries:
                                    Log(
                                        LogfileName,
                                        "Error: Rate limit persisted for tile Z,X,Y "
                                        + str(Z)
                                        + " "
                                        + str(X)
                                        + " "
                                        + str(Y),
                                    )

                                    Log(
                                        LogfileName,
                                        "Status:" + str(TileDownload.status_code),
                                    )

                                    Log(
                                        LogfileName,
                                        "URL:" + TileDownload.url,
                                    )

                                    Log(
                                        LogfileName,
                                        "Response headers:" + str(TileDownload.headers),
                                    )

                                    Run = False
                                else:
                                    Log(
                                        LogfileName,
                                        (
                                            "Rate limited for tile Z,X,Y "
                                            + str(Z)
                                            + " "
                                            + str(X)
                                            + " "
                                            + str(Y)
                                            + ". Retrying in "
                                            + f"{RetryAfterSeconds:.1f}"
                                            + " seconds."
                                        ),
                                    )

                                    sleep(RetryAfterSeconds)

                            elif TileDownload.status_code == 404:
                                RetryCounter += 1

                                if RetryCounter == MaxRetries:
                                    TileMissing = True
                                    MissingTilesThisMap += 1

                                    Log(
                                        LogfileName,
                                        "Warning: Tile Z,X,Y "
                                        + str(Z)
                                        + " "
                                        + str(X)
                                        + " "
                                        + str(Y)
                                        + " seems out of bounds (404)",
                                    )

                            else:
                                RetryCounter += 1

                                if RetryCounter == MaxRetries:
                                    Log(
                                        LogfileName,
                                        "Error: Failed to download tile Z,X,Y "
                                        + str(Z)
                                        + " "
                                        + str(X)
                                        + " "
                                        + str(Y),
                                    )

                                    Log(
                                        LogfileName,
                                        "Status:" + str(TileDownload.status_code),
                                    )

                                    Log(
                                        LogfileName,
                                        "URL:" + TileDownload.url,
                                    )

                                    Log(
                                        LogfileName,
                                        "Request headers:"
                                        + str(TileDownload.request.headers),
                                    )

                                    Log(
                                        LogfileName,
                                        "Response headers:" + str(TileDownload.headers),
                                    )

                                    Run = False  # Currently no way to handle errors more gracefully - just stop the program and retry with next run.
                                else:
                                    # Change 4: Back off exponentially before retrying other HTTP errors
                                    BackoffSeconds = CalculateRetryBackoff(
                                        ReadSpacing,
                                        RetryCounter,
                                    )

                                    Log(
                                        LogfileName,
                                        (
                                            "HTTP "
                                            + str(TileDownload.status_code)
                                            + " for tile Z,X,Y "
                                            + str(Z)
                                            + " "
                                            + str(X)
                                            + " "
                                            + str(Y)
                                            + ". Retrying in "
                                            + f"{BackoffSeconds:.1f}"
                                            + " seconds."
                                        ),
                                    )

                                    sleep(BackoffSeconds)

                            if not Run:
                                break

                        LevelTilesProcessed += 1
                        OverallTilesProcessed += 1

                        CurrentTime: float = time.monotonic()

                        if (
                            CurrentTime - LastProgressTime >= ProgressInterval
                            or LevelTilesProcessed == LevelTilesToProcess
                            or not Run
                        ):
                            ElapsedSeconds: float = CurrentTime - ProgressStartTime

                            if ElapsedSeconds > 0:
                                ProcessingSpeed: float = (
                                    OverallTilesProcessed / ElapsedSeconds
                                )
                            else:
                                ProcessingSpeed = 0

                            RemainingTiles: int = max(
                                TotalTilesToProcess - OverallTilesProcessed,
                                0,
                            )

                            if ProcessingSpeed > 0:
                                ETASeconds: float = RemainingTiles / ProcessingSpeed

                                ETAString: str = str(
                                    datetime.timedelta(seconds=int(ETASeconds))
                                )
                            else:
                                ETAString = "unknown"

                            if LevelTilesToProcess > 0:
                                LevelPercent: float = (
                                    LevelTilesProcessed / LevelTilesToProcess * 100
                                )
                            else:
                                LevelPercent = 100

                            if TotalTilesToProcess > 0:
                                OverallPercent: float = (
                                    OverallTilesProcessed / TotalTilesToProcess * 100
                                )
                            else:
                                OverallPercent = 100

                            Log(
                                LogfileName,
                                (
                                    f"{datetime.datetime.now():%H:%M:%S} | "
                                    f"z{Z} "
                                    f"{LevelTilesProcessed}/"
                                    f"{LevelTilesToProcess} "
                                    f"({LevelPercent:.1f}%) | "
                                    f"overall "
                                    f"{OverallTilesProcessed}/"
                                    f"{TotalTilesToProcess} "
                                    f"({OverallPercent:.1f}%) | "
                                    f"downloaded "
                                    f"{SuccessfulTilesThisMap} | "
                                    f"missing "
                                    f"{MissingTilesThisMap} | "
                                    f"{ProcessingSpeed:.2f} tiles/s | "
                                    f"ETA {ETAString}"
                                ),
                            )

                            LastProgressTime = CurrentTime

                        if not Run:
                            break

                    if not Run:
                        break

                if not Run:
                    break

            # On SIGTERM or SIGINT and after full loop save DB
            if len(VectorTiles) > 0:
                TotalTileCount = WriteToDB(
                    MBtilesDB,
                    VectorTiles,
                    LogfileName,
                    TotalTileCount,
                )

            if SessionTileCount > 0:
                WriteGlobalStatus(
                    ProcessStateFile,
                    SourceCounter,
                    X,
                    Y,
                    Z,
                    TotalTileCount,
                )

            VectorTiles = ()

            if Run:
                FullLoops += 1
                WriteMapStatus(MapStatusFile, FullLoops)

                subprocess.run(
                    [
                        "mbtiles",
                        "validate",
                        "--agg-hash",
                        "update",
                        str(MBtilesDB),
                    ],
                    check=True,
                )

                MBtilesCopy: Path = MBtilesDB.with_name(
                    MBtilesDB.stem + str(FullLoops) + MBtilesDB.suffix
                )

                shutil.copy(MBtilesDB, MBtilesCopy)

                Log(
                    LogfileName,
                    "Whole area processed completely - copy created and start next mapsource.",
                )

                SourceCounter += 1
                TotalTileCount = 0
                MapRun = False
                PickupDone = True

                if SourceCounter >= len(MapSources):
                    if ProcessStateFile.exists():
                        ProcessStateFile.unlink()

                    Run = False

    RequestSession.close()

    Log(
        LogfileName,
        "Shutdown received or error occured - graceful exit successfull.",
    )

    Log(
        LogfileName,
        "------ Download ended at "
        + str(datetime.datetime.now())
        + " after getting "
        + str(SessionTileCount)
        + " tiles. ------",
    )


if __name__ == "__main__":
    typer.run(main)

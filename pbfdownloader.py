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
# 		"ReadSpacing": 1.5                                                                                  # wait-time between to requests in seconds
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

import requests
import math
import signal
from time import sleep
from collections import namedtuple
import shutil
import datetime
import gzip
import os
import json
import sqlite3

######## CONFIG start ######

# Write to DB every X new tiles collected
WriteInterval = 250

# Files for storing the status
ProcessStateFile = "./DownloadState.txt"
LogfileName = "./download.log"

# User Agent for the web requests - some services block non-browser user agents
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0"
}

######## CONFIG end ######

with open("./mapconfig.json") as json_data:
    Maplist = json.load(json_data)
    json_data.close()

MapSources = list(Maplist.keys())

ServerPartNumber = 0
SessionTileCount = 0
TotalTileCount = 0
SourceCounter = 0
VectorTiles = ()

Status = namedtuple("Status", ["Source", "X", "Y", "Z", "TotalTileCount"])

Run = True

######## INIT end #######


# From https://medium.com/@ty2/how-to-calculate-number-of-tiles-in-a-bounding-box-for-openstreetmaps-4bf8c3b767ac
# And be aware of Y-axis deviation: https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md
# Tile number check: https://labs.mapbox.com/what-the-tile/
def deg2num(lat_deg, lon_deg, zoom):
    lat_rad = math.radians(lat_deg)
    n = 2.0**zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) * n / 2.0)
    return (xtile, ytile)


def WriteGlobalStatus(File, Source, X, Y, Z, TotalTileCount):
    with open(File, "w") as StatusFile:
        StatusFile.write(str(Source) + "\n")
        StatusFile.write(str(Z) + "\n")
        StatusFile.write(str(X) + "\n")
        StatusFile.write(str(Y) + "\n")
        StatusFile.write(str(TotalTileCount) + "\n")
        StatusFile.close()
    print("Updated Download State")


def ReadGlobalStatus(File):
    with open(File, "r") as StatusFile:
        Source = int(StatusFile.readline())
        Z = int(StatusFile.readline())
        X = int(StatusFile.readline())
        Y = int(StatusFile.readline())
        TotalTileCount = int(StatusFile.readline())
        StatusFile.close()
    return Status(Source=Source, X=X, Y=Y, Z=Z, TotalTileCount=TotalTileCount)


def WriteMapStatus(File, FullLoops):
    with open(File, "w") as StatusFile:
        StatusFile.write(str(FullLoops) + "\n")
        StatusFile.close()


def ReadMapStatus(File):
    with open(File, "r") as StatusFile:
        FullLoops = int(StatusFile.readline())
        StatusFile.close()
    return FullLoops


def Log(LogfileName, Message):
    print(Message)
    with open(LogfileName, "a") as Logfile:
        Logfile.write(Message + "\n")
        Logfile.close()


def CountDatabaseTiles(database_file):
    with sqlite3.connect(database_file) as connection:
        result = connection.execute("SELECT COUNT(*) FROM tiles").fetchone()

    return result[0]


def WriteToDB(DatabaseFile, TileList, LogfileName, TotalTileCount):
    with sqlite3.connect(DatabaseFile) as database:
        before_count = database.execute("SELECT COUNT(*) FROM tiles").fetchone()[0]

        database.executemany(
            """
            INSERT OR REPLACE INTO tiles
            (zoom_level, tile_column, tile_row, tile_data)
            VALUES (?, ?, ?, ?)
            """,
            TileList,
        )

        after_count = database.execute("SELECT COUNT(*) FROM tiles").fetchone()[0]

    unique_added = after_count - before_count
    overwritten = len(TileList) - unique_added

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


def handler_stop_signals(signum, frame):
    global Run
    Run = False


######## Procedures end #########

Log(LogfileName, "------ Start at " + str(datetime.datetime.now()) + " ------")

signal.signal(signal.SIGINT, handler_stop_signals)
signal.signal(signal.SIGTERM, handler_stop_signals)

if os.path.exists(ProcessStateFile):
    PickupStatus = ReadGlobalStatus(ProcessStateFile)
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
    min_z = PickupStatus.Z
    TotalTileCount = PickupStatus.TotalTileCount
    SourceCounter = PickupStatus.Source
    PickupDone = PickupStatus.X == 0
    MapRun = True
else:
    PickupDone = True

while Run:

    DownloadURL = Maplist[MapSources[SourceCounter]]["DownloadURL"]
    ServerParts = Maplist[MapSources[SourceCounter]]["ServerParts"]
    MBtilesDB = Maplist[MapSources[SourceCounter]]["MBtilesDB"]
    MapName = Maplist[MapSources[SourceCounter]]["Name"]
    max_z = Maplist[MapSources[SourceCounter]]["max_z"]
    BoundingBox = Maplist[MapSources[SourceCounter]]["BoundingBox"]
    ReadSpacing = Maplist[MapSources[SourceCounter]]["ReadSpacing"]
    min_z0 = Maplist[MapSources[SourceCounter]]["min_z"]
    MapStatusFile = "./" + MapSources[SourceCounter] + "_status.txt"
    if os.path.exists(MapStatusFile):
        FullLoops = ReadMapStatus(MapStatusFile)
    else:
        FullLoops = 0
    if PickupDone:
        min_z = min_z0
    MapRun = True

    NumberOfServerParts = len(ServerParts)
    MaxRetries = NumberOfServerParts

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

    while MapRun and Run:

        Log(
            LogfileName,
            "Now processing " + MapSources[SourceCounter] + " (" + MBtilesDB + ")",
        )

        for Z in range(min_z, max_z + 1):

            # Coordinates for Google-style URLs
            LowerLeft = deg2num(
                BoundingBox[1], BoundingBox[0], Z  # min latitude  # min longitude
            )

            UpperRight = deg2num(
                BoundingBox[3], BoundingBox[2], Z  # max latitude  # max longitude
            )

            min_x = LowerLeft[0]
            max_x = UpperRight[0]
            min_y = UpperRight[1]
            max_y = LowerLeft[1]

            # Google-Scheme to Mapbox/TMS Y is: Y_mapbox = 2^Z - 1 - Y_tms
            Yconversion = 2**Z - 1

            NumberOfTiles = (max_x - min_x + 1) * (max_y - min_y + 1)

            WriteGlobalStatus(
                ProcessStateFile, SourceCounter, min_x, min_y, Z, TotalTileCount
            )

            Log(
                LogfileName,
                "Will download Level "
                + str(Z)
                + " - number of tiles: "
                + str(NumberOfTiles),
            )

            if PickupDone:
                StartY = min_y
            else:
                StartY = PickupStatus.Y

            for Y in range(StartY, max_y + 1):

                if PickupDone:
                    StartX = min_x
                else:
                    StartX = PickupStatus.X
                    PickupDone = True

                for X in range(StartX, max_x + 1):

                    RetryCounter = 0

                    while RetryCounter < MaxRetries:
                        ServerPart = ServerParts[ServerPartNumber]
                        ServerPartNumber += 1
                        if ServerPartNumber == NumberOfServerParts:
                            ServerPartNumber = 0

                        URL = (
                            DownloadURL.replace("{server}", ServerPart)
                            .replace("{x}", str(X))
                            .replace("{y}", str(Y))
                            .replace("{z}", str(Z))
                        )
                        TileDownload = requests.get(URL, headers=headers)

                        if TileDownload.status_code == 200:
                            TileData = gzip.compress(TileDownload.content)
                            VectorTiles += ((Z, X, Yconversion - Y, TileData),)
                            SessionTileCount += 1

                            RetryCounter = MaxRetries  # Flag as done

                            if len(VectorTiles) == WriteInterval:
                                TotalTileCount = WriteToDB(
                                    MBtilesDB, VectorTiles, LogfileName, TotalTileCount
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
                        elif TileDownload.status_code == 404:
                            RetryCounter += 1
                            if RetryCounter == MaxRetries:
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
                                Log(LogfileName, "URL:" + TileDownload.url)
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

                        if not Run:
                            break

                    if Run:
                        sleep(ReadSpacing)
                    else:
                        break

                if not Run:
                    break

            if not Run:
                break

        # On SIGTERM or SIGINT and after full loop save DB
        if len(VectorTiles) > 0:
            TotalTileCount = WriteToDB(
                MBtilesDB, VectorTiles, LogfileName, TotalTileCount
            )
        if SessionTileCount > 0:
            WriteGlobalStatus(ProcessStateFile, SourceCounter, X, Y, Z, TotalTileCount)
        VectorTiles = ()

        if Run:
            FullLoops += 1
            WriteMapStatus(MapStatusFile, FullLoops)
            shutil.copy(
                MBtilesDB, MBtilesDB.replace(".mbtiles", str(FullLoops) + ".mbtiles")
            )
            Log(
                LogfileName,
                "Whole area processed completely - copy created and start next mapsource.",
            )

            SourceCounter += 1
            TotalTileCount = 0
            MapRun = False
            PickupDone = True

            if SourceCounter >= len(MapSources):
                if os.path.exists(ProcessStateFile):
                    os.remove(ProcessStateFile)

                Run = False

Log(LogfileName, "Shutdown received or error occured - graceful exit successfull.")
Log(
    LogfileName,
    "------ Download ended at "
    + str(datetime.datetime.now())
    + " after getting "
    + str(SessionTileCount)
    + " tiles. ------",
)

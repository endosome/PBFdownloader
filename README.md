# PBFdownloader
A "fair use" friendly vector map download script that creates [MBtiles](https://github.com/mapbox/mbtiles-spec) databases

## Purpose

This script downloads large area vector maps from tile-based web map services that provide vector tiles in [protobuf](https://protobuf.dev/) format. The script does so at low download rates, to not unduely burden the web service, to avoid hitting rate limits or even trigger service providers to make their services unavailable or put them behind walls. It is supposed to run in the background while the computer is running anyhow, and will download the maps over days or even weeks. It can be configured to download several maps in sequence - when the last one is done, it will start updating the first one again.

## Configuration

Configuration is done with the JSON file `mapconfig.json` in the directory you run the script from. Format is:

```
{
  "FreelyChosenMapName": {
    "DownloadURL": "https://{server}.mapsource.tld/path/to/pbf/tiles/{z}/{x}/{y}/tile.pbf?any=get&var=needed",
    "ServerParts": ["server1", "server2", ...],
    "BoundingBox": [min_lat, min_lon, max_lat, max_lon],
    "MBtilesDB": "/path/to/your/MBtiles-file.mbtiles",
    "Name": "Mapname in the MBtiles DB",
    "min_z": 0,
    "max_z": 14,
    "ReadSpacing": 1.5
  },
  "NextMap": {...},
  ...
}
```

An example file is provided in this repository - please note that the SwissTopo example is untested and I included it only because SwissTopo uses load balancing and I needed an example for this. SwissTopo data can be [downloaded as ready-made MBtiles](https://docs.geo.admin.ch/visualize-data/vector-tiles.html#gettilesets). Isn't that cool?

## Prerequisites

Aside from some common python libraries, you'll need the [pymbtiles](https://github.com/consbio/pymbtiles) library from the [Conservation Biology Institute](https://consbio.org/) - just run `pip install pymbtiles`. Thanks to the institute for providing this library!

## Important Notice

If you use this script, I'd kindly ask you to respect and adhere to the usage conditions and policies of the web map service providers. Failing to do so may result in services getting offline, behind authentication or pay walls etc., which would make everyone's life more difficult.

## Running the Script

I run it via cron job - my job looks like this:

`@reboot sleep 30 && cd /home/<user>/PBFdownloader && /path/to/python/virtual/environment/bin/python3 pbfdownloader.py > /home/<user>/PBFdownloader/jobrun.log 2>&1`

where `/home/<user>/PBFdownloader/` is the path to the script.

## License

Script is under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/). Have fun!

## More Detailed Instructions and Information

For more background and explanations, visit [my blog](https://projects.webvoss.de/2025/09/27/fair-use-download-of-large-vector-maps/).

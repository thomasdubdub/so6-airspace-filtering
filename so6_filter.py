from datetime import datetime
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString, Polygon, MultiPolygon


def make_time(row):
    datetime_str = str(row["date_begin"]) + str(row["time_begin"])
    return pd.datetime.strptime(datetime_str, "%y%m%d%H%M%S")


def make_line(row):
    coordinates = [
        (row["lon_begin"], row["lat_begin"]),
        (row["lon_end"], row["lat_end"]),
    ]
    return LineString(coordinates)


class So6:
    def __init__(self, file_path):
        columns = [
            "segment_identifier",
            "flight_origin",
            "flight_destination",
            "aircraft_type",
            "time_begin",
            "time_end",
            "fl_begin",
            "fl_end",
            "status",
            "callsign",
            "date_begin",
            "date_end",
            "lat_begin",
            "lon_begin",
            "lat_end",
            "lon_end",
            "trajectory_id",
            "sequence",
            "length",
            "parity",
        ]

        df = pd.read_csv(file_path, sep=" ", header=None, names=columns,)
        # filtering common so6 errors
        df.query("length > 0", inplace=True)
        df.query("time_begin>=10000", inplace=True)
        df.query("time_end>=10000", inplace=True)
        self.df = df.copy()  # copy used to export

        df.loc[:, "t"] = df.apply(make_time, axis=1)
        df.drop_duplicates(
            ["t", "trajectory_id"], inplace=True
        )  # no filtering normally

        lat_lon_cols = ["lat_begin", "lon_begin", "lat_end", "lon_end"]
        df.loc[:, lat_lon_cols] = df[lat_lon_cols].apply(lambda x: x / 60)
        cols_to_drop = [
            "segment_identifier",
            "flight_origin",
            "flight_destination",
            "aircraft_type",
            "time_begin",
            "time_end",
            "fl_begin",
            "fl_end",
            "status",
            "callsign",
            "date_begin",
            "date_end",
            "sequence",
            "length",
            "parity",
        ]
        df.drop(cols_to_drop, axis=1, inplace=True)
        df.set_index("t", inplace=True)
        df.loc[:, "geometry"] = df.apply(make_line, axis=1)
        df.drop(["lat_begin", "lon_begin", "lat_end", "lon_end"], axis=1, inplace=True)

        self.gdf = gpd.GeoDataFrame(df, geometry="geometry", crs={"init": "epsg:4326"},)
        self.ids = set(self.gdf.trajectory_id.unique())
        self.trajs = self.gdf.dissolve(by="trajectory_id")

    def clip(self, polygon_list):
        multipoly = MultiPolygon(polygon_list)
        intersection = self.trajs.intersects(multipoly)
        intersecting_flights = intersection[intersection]  # Series
        self.ids = set(intersecting_flights.index)
        print(f"Nb of trajs intersecting polygons: {len(self.ids)}")
        return

    def export(self, file_name):
        filter_df = self.df.query("trajectory_id in @self.ids")
        filter_df.to_csv(file_name, sep=" ", header=None, index=False)
        print(f"{file_name} created")
        return

    def __repr__(self):
        return f"Nb of trajs in so6: {len(self.ids)}"



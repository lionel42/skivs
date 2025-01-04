# %%
import pandas as pd
from datetime import datetime, timedelta
import requests
import json
import pytz
from dataclasses import dataclass
import concurrent.futures


# %%
@dataclass
class Connection:
    key: str
    start: datetime
    end: datetime
    duration: float
    transfers: int
    daytype: str
    is_return: bool = False

    def __repr__(self):
        return f"Connection({self.key} {self.start:%H:%M} -> {self.end:%H:%M}, {self.duration} min, {self.transfers} transfers, {'BACK' if self.is_return else 'GO'})"


# %%
df = pd.read_csv("tranportations.csv", comment="#", quotechar='"', sep=",")
df


# %%

# Get the connexions of the day
link_template = "http://transport.opendata.ch/v1/connections?from={station_start}&to={station_destination}&date={date:%Y-%m-%d}&time=04:00&limit=16"


# %%
# Localize the time to Europe/Zurich
tz = pytz.timezone("Europe/Zurich")
today = datetime.now(tz)
next_saturday = today + timedelta(days=(5 - today.weekday()) % 7)
next_sunday = today + timedelta(days=(6 - today.weekday()) % 7)
next_weekday = today + timedelta(days=(7 - today.weekday()) % 7)
schedules = {
    "weekday": next_weekday,
    "saturday": next_saturday,
    "sunday": next_sunday,
}

ALL_CONNECTIONS = []


def read_valid_connections(
    connections: dict[str, any],
    start_time: datetime,
    end_time: datetime,
    is_leave: bool = False,
    max_duration: float | None = None,
    max_transfers: int | None = None,
) -> list[Connection]:
    valid_connections = []
    for conn in connections:
        dt_start = datetime.fromisoformat(conn["from"]["departure"]).astimezone(tz)
        dt_end = datetime.fromisoformat(conn["to"]["arrival"]).astimezone(tz)

        dt = dt_start if is_leave else dt_end

        if dt < start_time or dt > end_time:
            continue
        duration = (dt_end - dt_start).seconds / 60
        if max_duration is not None and duration > max_duration:
            continue
        if max_transfers is not None and conn["transfers"] > max_transfers:
            continue
        valid_connections.append(
            Connection(
                key=f"{conn['from']['station']['name']} -> {conn['to']['station']['name']}",
                start=dt_start,
                end=dt_end,
                duration=duration,
                transfers=conn["transfers"],
                is_return=is_leave,
                daytype=(
                    "weekday"
                    if dt.weekday() < 5
                    else ("saturday" if dt.weekday() == 5 else "sunday")
                ),
            )
        )
    return valid_connections


def calculate_frequencies(
    station_start: str,
    station_destination: str,
    **kwargs,
):
    frequencies = {"go": {}, "back": {}}

    for key, day in schedules.items():
        print(key, day.strftime("%A %d %B %Y"))
        start_time_arrival = day.replace(hour=8, minute=0)
        end_time_arrival = day.replace(hour=14, minute=0)

        start_time_leave = day.replace(hour=11, minute=0)
        end_time_leave = day.replace(hour=17, minute=0)

        link_url = link_template.format(
            station_start=station_start,
            station_destination=station_destination,
            date=day,
        )
        response = requests.get(link_url)
        response_json = response.json()
        if "connections" not in response_json:
            print(f"No connections found for {station_start} -> {station_destination}")
            continue
        connections_go = read_valid_connections(
            response_json["connections"],
            start_time_arrival,
            end_time_arrival,
            **kwargs,
        )
        ALL_CONNECTIONS.extend(connections_go)

        frequencies["go"][key] = round(
            len(connections_go)
            / ((end_time_arrival - start_time_arrival).seconds / 3600),
            2,
        )

        link_url = link_template.format(
            station_start=station_start,
            station_destination=station_destination,
            date=day,
        )
        response = requests.get(link_url)
        response_json = response.json()
        connections_back = read_valid_connections(
            response_json["connections"],
            start_time_leave,
            end_time_leave,
            is_leave=True,
            **kwargs,
        )
        ALL_CONNECTIONS.extend(connections_back)

        frequencies["back"][key] = round(
            len(connections_back)
            / ((end_time_leave - start_time_leave).seconds / 3600),
            2,
        )
    return frequencies


# %%


def process_row(row):
    station_start = row["stop_departure"]
    station_destination = row["stop_arrival"]
    print(f"{station_start} -> {station_destination}")

    frequencies = calculate_frequencies(
        station_start,
        station_destination,
        # Avoid wierd connections
        max_duration=row["duration(min)"] + 10,
        # Also include when walking is considered as a transfer
        max_transfers=0 if row["change"] is None else row["change"] + 1,
    )
    # Add the frequencies to the dataframe
    average_fequency = round(
        (sum(frequencies["go"].values()) + sum(frequencies["back"].values())) / 6, 2
    )
    return average_fequency


results = [process_row(row) for _, row in df.iterrows()]

# Update the dataframe with the processed rows
df["frequency"] = None
for index, row in enumerate(results):
    df.at[index, "frequency"] = row


# %%
df_connections = pd.DataFrame(ALL_CONNECTIONS)
df_connections.head(20)
# %%
df_connections.to_csv("connections.csv", index=False)

# %%
df.to_csv("tranportations_with_frequencies.csv", index=False)
# %%

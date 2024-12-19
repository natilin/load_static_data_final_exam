import pandas as pd


def custom_to_datetime(series, fmt, threshold):
    dates = pd.to_datetime(series, format=fmt, errors='coerce')
    return dates.apply(lambda x: x.replace(year=x.year - 100) if x.year > threshold else x)


def calculate_death_rate(kills, injuries) -> int:
    if not kills:
        kills = 0
    if not injuries:
        injuries = 0
    return (kills * 2) + injuries


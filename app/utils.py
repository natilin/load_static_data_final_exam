import pandas as pd
import math


def custom_to_datetime(series, fmt, threshold):
    dates = pd.to_datetime(series, format=fmt, errors='coerce')
    return dates.apply(lambda x: x.replace(year=x.year - 100) if x.year > threshold else x)


def calculate_death_rate(kills, injuries) -> int:
    kills = 0 if math.isnan(kills) else kills
    injuries = 0 if math.isnan(injuries)else injuries
    return (kills * 2) + injuries


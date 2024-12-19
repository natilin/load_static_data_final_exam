import pandas as pd


def custom_to_datetime(series, fmt, threshold=2009):
    dates = pd.to_datetime(series, format=fmt, errors='coerce')
    return dates.apply(lambda x: x.replace(year=x.year - 100) if x.year > threshold else x)

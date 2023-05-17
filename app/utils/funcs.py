#!/usr/bin/env python3

import re
import datetime


def get_date_from_filename(filename) -> datetime:
    """return datetime from filename"""
    date_pattern = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")
    matched = date_pattern.search(filename)
    if not matched:
        return None
    y, m, d = map(int, matched.groups())
    return datetime.date(y, m, d)


def select_filename(filenames) -> str:
    """return path based on filename"""
    dates = (get_date_from_filename(fn) for fn in filenames)
    dates = (d for d in dates if d is not None)
    last_date = max(dates)
    last_date = last_date.strftime("%Y-%m-%d")
    filenames = [fn for fn in filenames if last_date in fn]
    return "".join(filenames)

from hashlib import md5
from typing import Iterable, Optional

import pandas as pd


def get_md5(input_iterable: Iterable, delimiter: str = "|") -> str:
    full_str = f"{delimiter}".join(map(str, input_iterable)).encode("utf-8")
    md5_hash = md5(full_str).hexdigest()
    return md5_hash


def get_md5_from_df(
    input_dataframe: pd.DataFrame, columns: Optional[Iterable[str]] = None
) -> pd.Series:
    in_df = input_dataframe[columns] if columns is not None else input_dataframe
    md5_hashes = in_df.apply(lambda row: get_md5(row), axis=1)
    return md5_hashes


def add_hash_column(
    input_dataframe: pd.DataFrame,
    hash_column_name: str,
    columns: Optional[Iterable[str]] = None,
) -> pd.Series:
    md5_row = get_md5_from_df(input_dataframe=input_dataframe, columns=columns)
    out_df = input_dataframe.copy()
    out_df[hash_column_name] = md5_row
    return out_df

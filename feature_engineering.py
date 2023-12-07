from datetime import datetime
from typing import List
import dask.bag as db

import pandas as pd


def get_vector_total(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Calculates the magnitude of a vector based on a set of columns. The caluclation is sqrt(col1^2 + col2^2 + ... + coln^2)

    :param df: Dataframe to calculate vector total on
    :param cols: Columns to calculate vector total on

    :return: Series with vector total calculated
    """
    return df[cols].pow(2).sum(1).pow(0.5)


def get_variable_change_over_time(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Calculates the change in a variable over time. Here we assume that the input df has a 'dt' column
    which is the change in time between the current row and the previous row. Here we compute the change in the
    input col variable over the change in time.

    :param df: Dataframe containing data to calculate change in variable over time on
    :param col: Column to calculate change in variable over time on
    :return: Dataframe with change in variable over time calculated
    """
    return ((df[col] - df[col].shift(1)) / df['dt']).fillna(0)


def add_total_force_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds total force features to the dataframe. Total force is defined as the magnitude of the force vector calculated as
    sqrt(fx^2 + fy^2 + fz^2)

    :param df: Dataframe to add features to
    :return: Dataframe with total force features added
    """
    f1cols = []
    f2cols = []
    for col in df.columns:
        if str(col).startswith('fx') or str(col).startswith('fy') or str(col).startswith('fz'):
            cur_fcol = 'f' + str(col)[1:]
            if cur_fcol.endswith('1'):
                f1cols.append(cur_fcol)
            elif cur_fcol.endswith('2'):
                f2cols.append(cur_fcol)
    if f1cols:
        df['f1'] = get_vector_total(df, f1cols)
        f1cols.append('f1')
    if f2cols:
        df['f2'] = get_vector_total(df, f2cols)
        f2cols.append('f2')
    return df[['time', *f1cols, *f2cols]]




def add_velocity_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds velocity features to the dataframe. Velocity is defined as the change in position over change in time.
    Total Velocity is defined as the magnitude of the velocity vector calculated as sqrt(vx^2 + vy^2 + vz^2)

    :param df: Dataframe to add features to
    :return: Dataframe with velocity features added
    """
    v1cols = []
    v2cols = []

    for col in df.columns:
        if str(col).startswith('x') or str(col).startswith('y') or str(col).startswith('z'):
            cur_vcol = 'v' + str(col)
            df[cur_vcol] = get_variable_change_over_time(df, col)
            if cur_vcol.endswith('1'):
                v1cols.append(cur_vcol)
            elif cur_vcol.endswith('2'):
                v2cols.append(cur_vcol)
    if v1cols:
        df['v1'] = get_vector_total(df, v1cols)
        v1cols.append('v1')
    if v2cols:
        df['v2'] = get_vector_total(df, v2cols)
        v2cols.append('v2')

    return df[['time', *v1cols, *v2cols]]


def add_position_change_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds position change features to the dataframe. Position change is defined as the change in position

    :param df: Dataframe to add features to
    :return: Dataframe with position change features added
    """
    d1cols = []
    d2cols = []

    for col in df.columns:
        if str(col).startswith('x') or str(col).startswith('y') or str(col).startswith('z'):
            cur_dcol = 'd' + str(col)
            df[cur_dcol] = (df[col] - df[col].shift(1)).fillna(0)
            if cur_dcol.endswith('1'):
                d1cols.append(cur_dcol)
            elif cur_dcol.endswith('2'):
                d2cols.append(cur_dcol)

    if d1cols:
        df['d1'] = get_vector_total(df, d1cols)
        d1cols.append('d1')
    if d2cols:
        df['d2'] = get_vector_total(df, d2cols)
        d2cols.append('d2')

    return df[['time', *d1cols, *d2cols]]


def add_acceleration_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds acceleration features to the dataframe. Acceleration is defined as the change in velocity over change in time.
    Total Acceleration is defined as the magnitude of the acceleration vector calculated as
    sqrt(ax^2 + ay^2 + az^2)

    :param df: Dataframe to add features to
    :return: Dataframe with acceleration features added
    """
    a1cols = []
    a2cols = []

    for col in df.columns:
        if str(col).startswith('vx') or str(col).startswith('vy') or str(col).startswith('vz'):
            cur_acol = 'a' + str(col)[1:]
            df[cur_acol] = get_variable_change_over_time(df, col)
            if cur_acol.endswith('1'):
                a1cols.append(cur_acol)
            elif cur_acol.endswith('2'):
                a2cols.append(cur_acol)
    if a1cols:
        df['a1'] = get_vector_total(df, a1cols)
    if a2cols:
        df['a2'] = get_vector_total(df, a2cols)
    return df


def add_engineered_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds engineered features to the dataframe.

    We arbitrarily choose positive velocity to be the difference between the current position and the previous position.
    :param df: Dataframe to add features to
    :return: Dataframe with features added
    """

    df['timestamp'] = df['time'].apply(lambda x: datetime.timestamp(x))
    df['dt'] = df['timestamp'] - df['timestamp'].shift(1)
    db_funcs = db.from_sequence([add_velocity_features, add_total_force_features, add_position_change_features])

    # Calculate the velocity, acceleration, and position change features in parallel
    vdf, adf, pdf = db_funcs.map(lambda func: func(df))
    vadf = pd.merge(vdf, adf)
    vapdf = pd.merge(vadf, pdf)

    df = pd.merge(df, vapdf)
    """
    We assume that the acceleration is the change in velocity over the change in time. Therefore we must calculate
    the velocity before we can calculate the acceleration.
    """

    df = add_acceleration_features(df)
    return df

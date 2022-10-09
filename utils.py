import pandas as pd
from pandas import DataFrame, Timestamp
from datetime import datetime, timedelta
from typing import Generator, Literal


class EmptyDataframe(Exception):
    pass


def prepare_data(
    filepath_1='timebase_example.csv',
    filepath_2='closing_prices.csv'
) -> tuple[DataFrame]:

    timebase_df = pd.read_csv(filepath_1, sep=',', parse_dates=['timestamp'])
    prices_df = pd.read_csv(filepath_2, sep=',', parse_dates=['Date'])

    return timebase_df, prices_df


def filter_data(
    timebase_df: DataFrame,
    prices_df: DataFrame,
    **kwargs
) -> tuple[DataFrame]:

    TIMESTAMP_TEMPLATE = '%m/%d/%y %H:%M'

    start_time = datetime.strptime(kwargs.get('startTime'), TIMESTAMP_TEMPLATE)
    end_time = datetime.strptime(kwargs.get('endTime'), TIMESTAMP_TEMPLATE)
    trader_id = kwargs.get('traderId')
    symbol = kwargs.get('symbol')
    base_currency = kwargs.get('baseCurrency')

    # start filtering dataframes

    # get data in from requested time range
    timebase_df = timebase_df[(timebase_df.timestamp >= start_time) & (timebase_df.timestamp < end_time)]
    prices_df = prices_df[(prices_df.Date.dt.date >= start_time.date()) & (prices_df.Date.dt.date <= end_time.date())]
    # additional filtering if 'trader_id' was specified
    if trader_id:
        timebase_df = timebase_df[timebase_df.traderId == trader_id]
    # additional filtering if 'symbol' was specified
    if symbol:
        timebase_df = timebase_df[timebase_df.symbol == symbol]
    # additional filtering if 'base_currency' was specified
    if base_currency:
        timebase_df = timebase_df[timebase_df.base_currency == base_currency]

    if not len(timebase_df.index) or not len(prices_df.index):
        raise EmptyDataframe()

    return timebase_df, prices_df


def get_time_intervals(
    timebase_df: DataFrame,
    interval: Literal['day', 'hour']
) -> list[Timestamp]:

    #hour_round_lower = lambda t: t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
    #hour_round_higher = lambda t: t.replace(second=0, microsecond=0, minute=0, hour=t.hour + 1)

    INTERVALS = {
        'day': lambda start, end: pd.date_range(
            start.date(), 
            (end + timedelta(days=1)).date(), 
            freq='D'),
        'hour': lambda start, end: pd.date_range(
            start.floor(freq='H'), 
            end.ceil(freq='H'), 
            freq='H'),
    }

    start_time = timebase_df.min(axis=0)['timestamp']
    end_time = timebase_df.max(axis=0)['timestamp']

    return INTERVALS[interval](start_time, end_time)


def calculate_symbol_stats(symbol_df: DataFrame, usd_price: float) -> dict:

    data = {
        'trades_num': len(symbol_df.index),
        'total_quantity_traded': 0,
        'net_quantity_traded': 0,
        'Vwap': 0,
        'Vwap_USD': 0,
        'profit_USD': 0
    }

    total_price_curr_quantity = 0
    for trade in symbol_df.itertuples():
        data['total_quantity_traded'] += trade.tradeQuantity
        data['net_quantity_traded'] += (trade.tradeQuantity if trade.side == 'BUY' else -(trade.tradeQuantity))
        tmp_price = trade.tradePrice * usd_price
        data['profit_USD'] += (tmp_price if trade.side == 'Sell' else -(tmp_price))
        total_price_curr_quantity += trade.tradeQuantity * trade.tradePrice

    data['Vwap'] = total_price_curr_quantity / data['trades_num']
    data['Vwap_USD'] = data['Vwap'] * usd_price

    return data


def get_usd_price(price_curr: str, prices_df: DataFrame) -> float:
    if price_curr == 'USD':
        return 1.0
    else:
        return prices_df.loc[prices_df.Product2Symbol == price_curr]['USDValueAtClose'].values[0]


def get_interval_stats(
    timebase_df: DataFrame,
    prices_df: DataFrame,
    start: Timestamp,
    end: Timestamp
) -> Generator[dict, None, None]:

    interval_df = timebase_df[(timebase_df.timestamp >= start) & (timebase_df.timestamp < end)]
    prices_df = prices_df[(prices_df.Date.dt.date >= start.date()) & (prices_df.Date.dt.date <= end.date())]
    symbols = interval_df.symbol.unique()

    for symbol in symbols:
        symbol_df = interval_df[interval_df.symbol == symbol]
        usd_price = get_usd_price(symbol_df.price_currency.values[0], prices_df)

        stats = calculate_symbol_stats(symbol_df, usd_price)
        stats['date'] = str(start.date())
        stats['symbol'] = symbol
        stats['interval_start'] = str(start)
        yield stats


def calculate_stats_by_intervals(
    timebase_df: DataFrame,
    prices_df: DataFrame,
    interval: Literal['day', 'hour']
) -> DataFrame:

    time_intervals = get_time_intervals(timebase_df, interval)
    res = []
    for i in range(len(time_intervals)-1):
        for stats in get_interval_stats(timebase_df, prices_df, time_intervals[i], time_intervals[i+1]):
            res.append(stats)

    result_df = DataFrame(
        data=res,
        columns=[
            'date',
            'interval_start',
            'symbol',
            'trades_num',
            'total_quantity_traded',
            'net_quantity_traded',
            'Vwap',
            'Vwap_USD',
            'profit_USD'
        ])

    return result_df


def get_data_set(
    filepath_1='timebase_example.csv',
    filepath_2='closing_prices.csv',
    **kwargs
) -> str:
    try:
        timebase_df, prices_df = prepare_data(filepath_1, filepath_2)
        timebase_df, prices_df = filter_data(timebase_df, prices_df, **kwargs)
        result_df = calculate_stats_by_intervals(timebase_df, prices_df, kwargs.get('interval'))
        return result_df.to_csv(index=False, float_format='%.8f')
    except EmptyDataframe:
        return ''

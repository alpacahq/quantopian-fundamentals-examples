from iexfinance.base import _IEXBase
from iexfinance import Stock
from urllib.parse import quote
from pylivetrader import *
import pandas as pd
import numpy as np
import json

def get_sector(sector_name):
    collection = SectorCollection(sector_name)
    return collection.fetch()

class SectorCollection(_IEXBase):

    def __init__(self, sector, **kwargs):
        self.sector = quote(sector)
        self.output_format = 'json'
        super(SectorCollection, self).__init__(**kwargs)

    @property
    def url(self):
        return '/stock/market/collection/sector?collectionName={}'.format(self.sector)

sectors = [
    'Basic Materials',
    'Consumer Cyclical',
    'Financial Services',
    'Real Estate',
    'Consumer Defensive',
    'Healthcare',
    'Utilities',
    'Communication Services',
    'Energy',
    'Industrials',
    'Technology'
]

def initialize(context):
    # Dictionary of stocks and their respective weights
    context.stock_weights = {}
    # Count of days before rebalancing
    context.days = 0
    # Number of sectors to go long in
    context.sect_numb = 2

    # Rebalance monthly on the first day of the month at market open
    # schedule_function(rebalance,
    #                   date_rule=date_rules.month_start(),
    #                   time_rule=time_rules.market_open())
    update_context(context)


def update_context(context):
    num_stocks = 50
    num_sectors_to_buy = 2

    sector_pe_ratios = {}
    sector_fundamental_dfs = {}
    for sector in sectors:
        fundamental_df = build_sector_fundamentals(sector)
        # We want to buy in the sectors with the highest average PE of their top companies.
        fundamental_df = fundamental_df.sort_values(by=['market_cap'], ascending=False)
        filtered_fundamental_df = get_filtered_fundamental_df(fundamental_df)
        sector_fundamental_dfs[sector] = filtered_fundamental_df
        sector_pe_ratios[sector] = filtered_fundamental_df['pe_ratio'][:num_stocks].mean()

    # Find the stocks for the sectors with the highest PE ratios.
    sector_pe_ratios = [(k, sector_pe_ratios[k]) for k in sorted(
            sector_pe_ratios,
            key=sector_pe_ratios.get,
            reverse=True
        )]
    context.stocks = []
    for i in range(0, num_sectors_to_buy):
        sector = sector_pe_ratios[i][0]
        print("Adding the {} sector to the order.".format(sector))
        # Get a list of the top stocks (by market cap) for the sector.
        sector_stocks = list(sector_fundamental_dfs[sector][:num_stocks].index.values)
        context.stocks += sector_stocks

def before_trading_start(context, data):
    update_context(context)


def get_filtered_fundamental_df(fundamental_df):
    return fundamental_df[
        (fundamental_df.quick_ratio >= 1) & \
        (fundamental_df.pe_ratio < 15) & \
        (fundamental_df.pb_ratio < 1.5)
    ]

def rebalance(context):
    # Exit all positions we wish to drop before starting new ones.
    for stock in context.portfolio.positions:
        if stock not in context.stocks:
            order_target_percent(stock, 0)
            #print("Selling stock {}".format(stock))

    # Create weights for each stock.
    weight = create_weights(context, context.stocks)

    # Rebalance all stocks to target weights.
    for stock in context.stocks:
        if weight != 0:
            #print("buying stock {} with {}%".format(stock, weight * 100))
            order_target_percent(symbol(stock), weight)

    print(get_open_orders())

def build_sector_fundamentals(sector):
    stocks = get_sector(sector)
    if len(stocks) == 0:
        raise ValueError("Invalid sector name: {}".format(sector))

    # First, filter out stocks with unknown PE ratios to minimize batch queries.
    stocks = [s for s in stocks if s['peRatio'] is not None]

    # IEX doesn't like batch queries for more than 100 symbols at a time.
    batch_idx = 0
    batch_size = 99
    fundamentals_dict = {}
    while batch_idx < len(stocks):
        symbol_batch = [s['symbol'] for s in stocks[batch_idx:batch_idx+batch_size]]
        stock_batch = Stock(symbol_batch)

        financials_json = stock_batch.get_financials()
        quote_json = stock_batch.get_quote()
        stats_json = stock_batch.get_key_stats()

        for symbol in symbol_batch:
            fundamentals_dict[symbol] = {}

            if len(financials_json[symbol]) < 1:
                # This can sometimes happen in case of recent markert suspensions.
                continue

            if quote_json[symbol]['latestPrice'] is None:
                # This indicates that the stock may have recently been made available.
                continue

            # Use only the most recent financial report for this stock.
            financials = financials_json[symbol][0]
            if financials['totalAssets'] is None or financials['currentAssets'] is None:
                # Ignore companies who reported no assets on their balance sheet.
                continue

            if stats_json[symbol]['sharesOutstanding'] == 0:
                # Company may have recently gone private, or there's some other issue.
                continue

            if quote_json[symbol]['marketCap'] is None or quote_json[symbol]['marketCap'] == 0:
                # Ignore companies IEX cannot report market cap for.
                continue

            # calculate PB ratio
            book_value = financials['totalAssets'] - financials['totalLiabilities'] \
                                        if financials['totalLiabilities'] else financials['totalAssets']
            book_value_per_share = book_value / stats_json[symbol]['sharesOutstanding']
            fundamentals_dict[symbol]['pb_ratio'] = quote_json[symbol]['latestPrice'] / book_value_per_share

            # approximate Morningstar's "quick ratio" as closely as IEX data can
            # if no debt is reported, just set it to 2, since the algorithm only cares that it's over 1.
            fundamentals_dict[symbol]['quick_ratio'] = financials['currentAssets'] / financials['currentDebt'] \
                                        if financials['currentDebt'] else 2

            fundamentals_dict[symbol]['pe_ratio'] = quote_json[symbol]['peRatio']
            fundamentals_dict[symbol]['market_cap'] = quote_json[symbol]['marketCap']
            fundamentals_dict[symbol]['shares_outstanding'] = stats_json[symbol]['sharesOutstanding']
        batch_idx += batch_size
    fundamentals_df = pd.DataFrame.from_dict(fundamentals_dict).T
    return fundamentals_df

def create_weights(context, stocks):
    """
        Takes in a list of securities and weights them all equally
    """
    # TODO: try weighting (inversely) by PEG ratio
    if len(stocks) == 0:
        return 0
    else:
        weight = 1.0/len(stocks)
        return weight


def handle_data(context, data):
    """
      Code logic to run during the trading day.
      handle_data() gets called every bar.
    """
    print('Handling data...')
    rebalance(context)
    exit()
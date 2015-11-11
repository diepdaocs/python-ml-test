#!/usr/bin/env python

import pandas as pd
from dateutil import relativedelta
from datetime import date
from peewee import *

__author__ = 'diepdt'

# database to save data
db = SqliteDatabase('OrderDB.db')


class BaseModel(Model):
    class Meta:
        database = db


class UserOrder(BaseModel):
    """
    Order model
    """
    order_id = IntegerField(primary_key=True)
    order_date = DateTimeField(formats='%d.%m.%Y', null=False)
    user_id = IntegerField(index=True, null=False)
    total_charges_usd = FloatField(null=False)


def date_parser(text):
    """
    Date parser
    Args:
        text: text to be parsed to datetime object
    Returns:
        datetime object
    """
    return pd.datetime.strptime(text, '%d.%m.%Y')


def load_data_from_file(file_path):
    """
    Load data from csv
    Args:
        file_path: file path
    Return:
        pandas DataFrame
    """
    data_frame = pd.read_csv(file_path, sep=';', parse_dates=['Order_Date'],
                             date_parser=date_parser)
    data_frame = data_frame[['Order_Date', 'User_ID']]
    return data_frame


def save_data_into_database(file_path):
    """
    Save csv order data to database
    Args:
        file_path: file path
    """
    # create database and table
    db.connect()
    if not UserOrder.table_exists():
        db.create_table(UserOrder)
    # insert data
    data_frame = pd.read_csv(file_path, sep=';')
    for idx, row in data_frame.iterrows():
        UserOrder.create_or_get(order_id=row['Order_ID'], order_date=row['Order_Date'], user_id=row['User_ID'],
                                total_charges_usd=row['Total_Charges_USD'].replace(',', '.'))


def load_order_data():
    """
    load data from database
    Returns:
        DataFrame of user data
    """
    data = []
    orders = UserOrder.select(UserOrder.order_date, UserOrder.user_id)
    for order in orders:
        data.append((date_parser(order.order_date), order.user_id))

    data_frame = pd.DataFrame(data=data, columns=['Order_Date', 'User_ID'])
    return data_frame


def main():
    # save data into database
    save_data_into_database('OrderDB.csv')
    # load order data
    data_frame = load_order_data()
    # sort order by date for easy user tracking
    data_frame.sort('Order_Date')
    # loop through data to tracking user buy (first_buy, second_buy, third_buy,...)
    data_frame['First_Buy'] = ''  # first buy info for group by count user later
    data_frame['User_Tracking'] = ''  # user tracking info (first_buy, second_buy,..) for group by count user later
    user_tracking = {}  # user tracking
    for idx, row in data_frame.iterrows():
        user_id = row['User_ID']
        cur_date = row['Order_Date']
        if user_id not in user_tracking:
            # first buy user (first_date for calculate distance month, tracking for filter duplicated buys in month)
            user_tracking[user_id] = {'first_date': cur_date, 'tracking': {1}}
            data_frame.loc[idx, 'User_Tracking'] = 1  # first buy
            data_frame.loc[idx, 'First_Buy'] = cur_date.strftime('%Y-%m')  # track user first month buy

        else:
            # buy again
            tracking = user_tracking[user_id]['tracking']
            first_date = user_tracking[user_id]['first_date']
            date_delta = relativedelta.relativedelta(date(cur_date.year, cur_date.month, 1),
                                                     date(first_date.year, first_date.month, 1))
            month_distance = date_delta.years * 12 + date_delta.months
            increment_id = month_distance + 1
            if month_distance >= 1 and increment_id not in tracking:
                # user buy in another month and have not added yet
                data_frame.loc[idx, 'User_Tracking'] = increment_id  # track buy again info
                data_frame.loc[idx, 'First_Buy'] = first_date.strftime('%Y-%m')  # track user first month buy info
                tracking.add(increment_id)

    # group and count user by month buy (1, 2, 3, 4,...)
    data_frame = data_frame[['User_Tracking', 'First_Buy']]
    data_frame['User_Count'] = ''
    data_frame = data_frame.groupby(['User_Tracking', 'First_Buy']).count()

    # extract result
    index = data_frame.index.levels[0][:-1]
    columns = data_frame.index.levels[1][1:]
    result = pd.DataFrame(index=index, columns=columns)
    for idx, row in data_frame.iterrows():
        row_index = idx[0]
        column_index = idx[1]
        if row_index and column_index:
            result.loc[row_index, column_index] = row['User_Count']

    result = result.fillna(0)

    result.to_csv('Result.csv', sep=';')

    return

if __name__ == '__main__':
    main()

# Copyright 2019 LI,JIE-YING. All rights reserved.
from app.sql_connection.postgres_connector import PostgresConnector
from app.grabber.twse_stock_price_grabber import TwseStockMonthPriceGrabber
import time


class OneStockGrabbingController(object):

    _start_year = 2010
    _start_month = 1
    _end_year = 2019
    _end_month = 12

    _recent_close_price = 0

    _stock_table_name_template = "TPE:{}"
    _create_table_statement_template = """
    CREATE TABLE "{}"(
    date DATE primary key,
    open NUMERIC(12, 2),
    high NUMERIC(12, 2),
    low NUMERIC(12, 2),
    close NUMERIC(12, 2),
    volume BIGINT,
    transaction BIGINT,
    created TIMESTAMP not null default now()
    );
    """

    def __init__(self):
        self.twse_stock_price_grabber = TwseStockMonthPriceGrabber()
        self.sql_connector = PostgresConnector()
        self._init_daily_price_stock_database_connection()

    def _init_daily_price_stock_database_connection(self):
        self._daily_price_stock_database_connector = self.sql_connector.daily_price_stock_database_connector
        self._daily_price_stock_database_cursor = self._daily_price_stock_database_connector.cursor()

    def grab_history_price(self, stock_number, grab_interval_sleep=False, use_luminati_proxy=False):

        print(stock_number + " 開始抓取歷史股價！！")
        self._insert_status_information_to_sql("history_price_grabber", "normal", stock_number, "0", "開始抓取歷史股價", "")

        current_table_name = self._stock_table_name_template.format(stock_number)
        self._create_daily_price_stock_table(current_table_name)

        for current_year in range(self._start_year, self._end_year + 1):

            for current_month in range(self._start_month, self._end_month + 1):

                if self._check_date_more_than_the_2019_8(current_year, current_month) is True:
                    break

                try:
                    self._grab_month_stock_price_and_insert_to_sql(stock_number, current_table_name, current_year, current_month, use_luminati_proxy)
                except KeyError as e:
                    print("股票代號：" + stock_number + "，年：" + str(current_year) + "月：" + str(current_month))
                    print(e.args)
                    print("很抱歉，沒有符合條件的資料!")

                if grab_interval_sleep is True:
                    time.sleep(10)

        print(stock_number + " 抓取成功！！！！")
        self._insert_status_information_to_sql("history_price_grabber", "normal", stock_number, "0", "抓取成功", "")

    def _insert_status_information_to_sql(self, status_source, status_type, stock_number, error_code, message, remark):
        insert_sql_statement = self._produce_insert_status_information_statement(status_source, status_type, stock_number, error_code, message, remark)
        self._daily_price_stock_database_cursor.execute(insert_sql_statement)
        self._daily_price_stock_database_connector.commit()

    @staticmethod
    def _produce_insert_status_information_statement(status_source, status_type, stock_number, error_code, message, remark):
        insert_sql_statement = """INSERT INTO "status_information" ("status_source", "status_type", "stock_number", "error_code", "message", "remark") 
            VALUES ('{status_source}', '{status_type}', '{stock_number}', '{error_code}', '{message}', '{remark}');"""
        return insert_sql_statement.format(
            status_source=status_source,
            status_type=status_type,
            stock_number=stock_number,
            error_code=error_code,
            message=message,
            remark=remark
        )

    def _create_daily_price_stock_table(self, table_string):
        if self._table_is_exist(table_string) is False:
            self._create_daily_price_stock_table_to_sql(table_string)
        else:
            print(table_string + "\t資料表存在")
            raise ValueError

    def _table_is_exist(self, table_string):
        select_sql = """SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';"""
        self._daily_price_stock_database_cursor.execute(select_sql)
        table_name_list = self._daily_price_stock_database_cursor.fetchall()

        for row in table_name_list:
            if row[0] == table_string:
                return True
        return False

    def _create_daily_price_stock_table_to_sql(self, table_string):
        create_sql = self._create_table_statement_template.format(table_string)
        self._daily_price_stock_database_cursor.execute(create_sql)
        self._daily_price_stock_database_connector.commit()

    def _grab_month_stock_price_and_insert_to_sql(self, stock_number, stock_table_name, year, month, use_luminati_proxy):
        history_data = self.twse_stock_price_grabber.grab_stock_price(stock_number=stock_number, year=year, month=month, use_luminati_proxy=use_luminati_proxy)
        self._insert_history_stock_price_to_sql(history_data, stock_table_name)

    def _insert_history_stock_price_to_sql(self, history_data, stock_table_name):
        for one_record in history_data:
            record_dict = self._separate_list_record_to_dict(one_record)
            record_dict = self._check_price_value(record_dict)
            insert_sql_statement = self._produce_insert_stock_price_statement(record_dict, stock_table_name)
            self._daily_price_stock_database_cursor.execute(insert_sql_statement)
        self._daily_price_stock_database_connector.commit()

    def _separate_list_record_to_dict(self, record):
        return {
            'date': self._date_transform_to_sql_type(record[0]),
            'open': record[3],
            'high': record[4],
            'low': record[5],
            'close': record[6],
            'volume': record[1].replace(",", ""),
            'transaction': record[8].replace(",", "")
        }

    @staticmethod
    def _date_transform_to_sql_type(date_string):
        date_string = date_string.split("/")
        return str(int(date_string[0]) + 1911) + "-" + date_string[1] + "-" + date_string[2]

    def _check_price_value(self, record_dict):
        if record_dict['close'] == "--":
            record_dict['open'] = self._recent_close_price
            record_dict['high'] = self._recent_close_price
            record_dict['low'] = self._recent_close_price
            record_dict['close'] = self._recent_close_price
        else:
            self._recent_close_price = record_dict['close']
        return record_dict

    @staticmethod
    def _produce_insert_stock_price_statement(record_dict, stock_table_name):
        insert_sql_statement = """INSERT INTO "{table_name}" ("date", "open", "high", "low", "close", "volume", "transaction") 
        VALUES ('{date}', {open}, {high}, {low}, {close}, {volume}, {transaction});"""
        return insert_sql_statement.format(
            table_name=stock_table_name,
            date=record_dict["date"],
            open=record_dict["open"],
            high=record_dict["high"],
            low=record_dict["low"],
            close=record_dict["close"],
            volume=record_dict["volume"],
            transaction=record_dict["transaction"]
        )

    @staticmethod
    def _check_date_more_than_the_2019_8(year, month):
        if year >= 2019 and month > 7:
            return True
        else:
            return False


if __name__ == "__main__":
    one_stock_grabbing_controller = OneStockGrabbingController()
    one_stock_grabbing_controller.grab_history_price("1715")

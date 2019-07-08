import time
from app.sql_connection.postgres_connector_by_sqlalchemy import PostgresConnectorBySqlalchemy
from app.grabber import yfinance_stock_price_grabber


class SystematicDailyStockPriceGrabberByYahoo:
    """
    媽的，結果 yahoo finance 根本不能用。
    一堆錯誤，棄用。
    """

    start = "2007-01-01"
    end = "2019-12-31"

    table_string_template = "TPE:{}"
    create_sql_statement = """
    CREATE TABLE "{}"(
    date DATE primary key,
    open NUMERIC(12, 2),
    high NUMERIC(12, 2),
    low NUMERIC(12, 2),
    close NUMERIC(12, 2),
    adj_close NUMERIC(12, 2),
    volume BIGINT,
    transaction INT,
    created TIMESTAMP not null default now()
    );
    """

    def __init__(self):
        self._db_connect_engine = PostgresConnectorBySqlalchemy()
        self._init_project_database_connector()
        self._init_daily_price_stock_database_connector_and_engine()

    def _init_project_database_connector(self):
        project_database_connector = self._db_connect_engine.project_database_engine.raw_connection()
        self._project_database_cursor = project_database_connector.cursor()

    def _init_daily_price_stock_database_connector_and_engine(self):
        self._daily_price_stock_database_engine = self._db_connect_engine.daily_price_stock_database_engine
        self.daily_price_stock_database_connector = self._daily_price_stock_database_engine.raw_connection()
        self._daily_price_stock_database_cursor = self.daily_price_stock_database_connector.cursor()

    def grab_history_price(self):

        list_of_need_grab_history_stock_price = self._get_need_grab_history_stock_list()

        print("company_basic_information table 裡要測試的股票總數：" + str(len(list_of_need_grab_history_stock_price)))

        for stock_item in list_of_need_grab_history_stock_price:

            stock_number = stock_item[1]
            current_table_name = self.table_string_template.format(stock_number)

            if not self._create_daily_price_stock_table(current_table_name):
                continue

            history_data = yfinance_stock_price_grabber.grab_stock_price_by_range(stock_number + ".TW", self.start, self.end)
            history_data = history_data.rename_axis("date")
            history_data = history_data.rename(columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Adj Close": "adj_close",
                "Volume": "volume"
                })
            history_data.to_sql(current_table_name, self._daily_price_stock_database_engine, if_exists='append')
            print(current_table_name + "抓取成功")

            time.sleep(5)

    def _get_need_grab_history_stock_list(self):
        select_sql = "SELECT serial_number, stock_number, status from company_basic_information WHERE status = 't' or status = 'v' ORDER BY serial_number;"
        self._project_database_cursor.execute(select_sql)
        need_grab_history_stock_list = self._project_database_cursor.fetchall()
        return need_grab_history_stock_list

    def _create_daily_price_stock_table(self, table_string):
        if self._table_is_exist(table_string):
            return False
        else:
            if self._create_daily_price_stock_table_to_sql(table_string):
                return True
            else:
                return False

    def _table_is_exist(self, table_string):

        select_sql = """SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';"""
        self._daily_price_stock_database_cursor.execute(select_sql)
        table_name_list = self._daily_price_stock_database_cursor.fetchall()

        for row in table_name_list:
            if row[0] == table_string:
                print(table_string + "\t資料表存在")
                return True

        print(table_string + "\t資料表不存在")
        return False

    def _create_daily_price_stock_table_to_sql(self, table_string):

        try:
            create_sql = self.create_sql_statement.format(table_string)

            self._daily_price_stock_database_cursor.execute(create_sql)
            self.daily_price_stock_database_connector.commit()

            print(table_string + "\t資料表創建成功")
            return True

        except BaseException as e:
            print(e.args)
            print(table_string + "\t資料表創建錯誤")
            return False


if __name__ == "__main__":
    systematicDailyStockPriceGrabberByYahoo = SystematicDailyStockPriceGrabberByYahoo()
    systematicDailyStockPriceGrabberByYahoo.grab_history_price()

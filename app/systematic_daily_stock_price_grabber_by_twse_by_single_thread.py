from app.sql_connection.postgres_connector import PostgresConnector
from app.one_stock_grabbing_controller import OneStockGrabbingController


class SystematicDailyStockPriceGrabberByTwseBySingleThread:

    def __init__(self):
        self._db_connector = PostgresConnector()
        self._init_project_database_connector()

    def _init_project_database_connector(self):
        self._project_database_connector = self._db_connector.project_database_connector
        self._project_database_cursor = self._project_database_connector.cursor()

    def grab_history_price(self):

        list_of_need_grab_history_stock_price = self._get_need_grab_history_stock_list()

        print("company_basic_information table 裡要測試的股票總數：" + str(len(list_of_need_grab_history_stock_price)))

        for stock_item in list_of_need_grab_history_stock_price:
            one_stock_grabbing_controller = OneStockGrabbingController()
            one_stock_grabbing_controller.grab_history_price(stock_item[1], grab_interval_sleep=True, use_luminati_proxy=False)

    def _get_need_grab_history_stock_list(self):
        select_sql = "SELECT serial_number, stock_number, status from company_basic_information WHERE status = 'b' ORDER BY serial_number;"
        self._project_database_cursor.execute(select_sql)
        need_grab_history_stock_list = self._project_database_cursor.fetchall()
        return need_grab_history_stock_list


if __name__ == "__main__":
    cla = SystematicDailyStockPriceGrabberByTwseBySingleThread()
    cla.grab_history_price()

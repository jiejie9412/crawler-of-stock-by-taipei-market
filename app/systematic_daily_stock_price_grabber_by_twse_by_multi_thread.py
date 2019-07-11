from app.sql_connection.postgres_connector_use_pool import PostgresConnectorUsePool
from app.one_stock_grabbing_controller import OneStockGrabbingController
import threading
import time

thread_set = set()


def one_stock_launcher(stock_number, daily_stock_price_database_connection_pool):
    thread_set.add(threading.current_thread())
    one_stock_grabbing_controller = OneStockGrabbingController(daily_stock_price_database_connection_pool)
    one_stock_grabbing_controller.grab_history_price(stock_number, grab_interval_sleep=False, use_luminati_proxy=True)
    thread_set.remove(threading.current_thread())


class SystematicDailyStockPriceGrabberByTwseByMultiThread:

    def __init__(self):
        self._db_connection_pool = PostgresConnectorUsePool()
        self._init_project_database_connector()

    def _init_project_database_connector(self):
        self._project_database_connector = self._db_connection_pool.project_database_connection_pool.getconn()
        self._project_database_cursor = self._project_database_connector.cursor()

    def grab_history_price(self):

        list_of_need_grab_history_stock_price = self._get_need_grab_history_stock_list()

        print("company_basic_information table 裡要測試的股票總數：" + str(len(list_of_need_grab_history_stock_price)))

        i = 0
        e = len(list_of_need_grab_history_stock_price)
        while i < e:
            if len(thread_set) < 30:
                thread = threading.Thread(
                    target=one_stock_launcher,
                    args=(
                        list_of_need_grab_history_stock_price[i][1],
                        self._db_connection_pool.daily_price_stock_database_connection_pool,
                    )
                )
                thread.start()
                time.sleep(3)
                i += 1

    def _get_need_grab_history_stock_list(self):
        select_sql = "SELECT serial_number, stock_number, status from company_basic_information WHERE status = 'grabbing' ORDER BY serial_number;"
        self._project_database_cursor.execute(select_sql)
        need_grab_history_stock_list = self._project_database_cursor.fetchall()
        return need_grab_history_stock_list


if __name__ == "__main__":
    cla = SystematicDailyStockPriceGrabberByTwseByMultiThread()
    cla.grab_history_price()

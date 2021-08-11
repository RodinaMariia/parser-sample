import bs4
import re
import inspect
import pandas as pd
import numpy as np
import parsing
import sqlite3
from abc import ABCMeta, abstractmethod
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait as wait


class StorageAdapter:
    """
    The parent class for organizing the storage of parsed information. Adapter initializing with empty identifiers.
    For correct working all internal parameters must be additionally initialize.

    :param _data_id: identifier of processed data block. Can be used as the primary id for main table
    or a foreign key for any adding table.
    :type _data_id: str or int

    :param is_new: flag for ney entries
    :type is_new: bool

    """
    __metaclass__ = ABCMeta

    def __init__(self):
        self._data_id = 0
        self.is_new = True

    @abstractmethod
    def add_table(self, label: str, n, data: pd.DataFrame):
        """
        Add information from Dataframe to the table identified by label.

        :param label: name of table keeping new data
        :type label: str

        :param n: serial number of adding data. Uses for identifying tables with the same names and different data.
        :type n: str-converted type

        :param data: table collecting the new data.
        :type data: pandas.DataFrame

        """
        pass

    @abstractmethod
    def add_main_data(self, label: str, data):
        """
        Add columns to the main table.

        :param label: new columns prefix or full name of the one adding column.
        :type label: str

        :param data: If data is dictionary it contains additional columns names and information.
        Otherwise it collects content of a one new column.
        :type data: dict or other type

        """
        pass

    @abstractmethod
    def add_error_data(self, url: str, page, func, err):
        """
        Add information to the table of errors.

        :param url: url of parsing page.
        :type url:  str

        :param page: parsing content.
        :type page: str like or some bs4/selenium parsing elements

        :param func: information about function caught an exception.
        :type func: FrameInfo

        :param err: caught exception.
        :type err: Exception like

        """
        pass

    @abstractmethod
    def roll_data(self):
        """Roll columns with equal names. Data collecting in one cell split by semicolon"""
        pass

    @abstractmethod
    def create_child_storage(self):
        """
        Create an instance of the current class initialized with specific parameters.

        :return: a new instance of the current class.
        :rtype: StorageAdapter
        """
        pass

    @abstractmethod
    def get_data(self):
        """Return keeping data in one piece"""
        pass

    def set_identifiers(self, new_id, *args):
        """Initialize required identifiers"""
        self._data_id = new_id
        return self


class StorageAdapterDataframe(StorageAdapter):
    """
    Storage collecting data in pandas DataFrames.

    :param _data_labels: dictionary with parsed data.
    :type _data_labels: dict

    """

    def __init__(self):
        super().__init__()
        self._data_labels = {'main': pd.DataFrame([[self._data_id]], columns=['id']),
                             'errors': pd.DataFrame(None, columns=['URL', 'parser', 'function', 'exception'])}

    def add_table(self, label: str, n: int, data):
        data['id'] = self._data_id
        new_label = label if self._data_labels.get(label) is None else str(label) + str(n)
        self._data_labels[new_label] = data

    def add_main_data(self, label: str, data):
        if isinstance(data, dict):
            cols = [label + '~' + one_key for one_key in data.keys()]
            self._data_labels['main'] = pd.concat(
                [self._data_labels['main'], pd.DataFrame([data.values()], columns=cols)], axis=1)
        else:
            self._data_labels['main'][label] = data

    def add_error_data(self, url, page, func, err):
        self._data_labels['errors'].loc[len(self._data_labels['errors'])] = [url, page, func, err]

    def roll_data(self):
        for one_element in self._data_labels.items():
            self._data_labels[one_element[0]] = one_element[1].groupby(level=0,
                                                                       axis=1).apply(lambda x: x.apply(join_for_columns,
                                                                                                       axis=1))

    def create_child_storage(self):
        return StorageAdapterDataframe()

    def get_data(self):
        return self._data_labels

    def set_identifiers(self, new_id, *args):
        super().set_identifiers(new_id)
        if id != 0:
            self._data_labels['main']['id'] = id
        return self


class StorageAdapterSQLite(StorageAdapter):
    """
      Storage collecting data in an outer database.

      :param db_connection: connection to the database.
      :type db_connection: sqlite3.Connection

      :param _main_table: dictionary with parsed data.
      :type _main_table: str

      :param _cursor: dictionary with parsed data.
      :type _cursor: sqlite3.Cursor

      ..note:: This application doesn't use multithreading and all steps goes one-by-one,
      so it's faster to use one connection and one cursor for all counting.
      """

    def __init__(self, db_connection: sqlite3.Connection, cursor: sqlite3.Cursor):
        super().__init__()
        self.db_connection = db_connection
        self._main_table = 'main'
        self._cursor = cursor

    def add_table(self, label, n, data):
        if len(data) != 0:
            table = preprocess_col_names(label)
            if not self._check_table_existion(table):
                self._create_table(table)
            self._add_rows(table, data)

    def add_main_data(self, label, data):
        if len(data) != 0:
            self._add_rows(self._main_table, data, column_name=label)

    def add_error_data(self, url, page, func, err):
        if not self._check_table_existion('errs'):
            self._create_table('errs')
        self._add_rows('errs', pd.DataFrame([[url, preprocess_str(page),
                                              func, preprocess_str(err)]],
                                            columns=['URL', 'parser', 'function', 'exception']))

    def roll_data(self):
        pass

    def create_child_storage(self):
        return StorageAdapterSQLite(self.db_connection, self._cursor)

    def get_data(self):
        pass

    def set_identifiers(self, new_id, data_type: str = ''):
        if len(data_type) == 0:
            raise ValueError('Data must be specified by parser type.')
        if new_id == 0:
            raise ValueError('Id must be significant.')

        self._main_table = 'main_' + data_type.strip()
        self._data_id = new_id
        if not self._check_table_existion(self._main_table):
            self._create_table(self._main_table)
        self.is_new = not self._check_record_existion(self._main_table, self._data_id)
        if self.is_new:
            self._cursor.execute("INSERT INTO {} (id) values ('{}')".format(self._main_table,
                                                                            self._data_id))
            self.db_connection.commit()
        return self

    def _check_table_existion(self, table):
        return True if len(self._cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='{}';".
                                                format(table)).fetchall()) > 0 else False

    def _check_record_existion(self, table, new_id, column_id='id'):
        """ Find record specified by id from column_id in the database. """
        return True if len(self._cursor.execute("SELECT {} FROM {} WHERE {} = '{}';".
                                                format(column_id, table,
                                                       column_id, new_id)).fetchall()) > 0 else False

    def _create_table(self, table):
        """Create table with basic columns. Some specific tables has it's own creation string."""
        if table.lower() == self._main_table:
            create_q = '''CREATE TABLE {}(
                    id TEXT PRIMARY KEY 
                    );'''.format(self._main_table)
        elif table.lower() == 'errs':
            create_q = '''CREATE TABLE errs(
                          id INTEGER PRIMARY KEY AUTOINCREMENT,
                          parent_id TEXT,
                          URL TEXT,
                          parser TEXT,
                          function TEXT,
                          exception TEXT
                          );'''
        else:
            create_q = '''CREATE TABLE {}(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    parent_id TEXT
                    );'''.format(table)
        self._cursor.execute(create_q)

    def _create_columns(self, table, columns):
        """Function take list of potential new columns and check their excision.
        Part of them which doesn't exist will be created."""
        new_columns = set(columns) - set(np.array(self._cursor.execute("PRAGMA table_info({})".format(table)).
                                                  fetchall())[:, 1])
        [self._cursor.execute("ALTER TABLE {} ADD COLUMN {} TEXT".format(table, col)) for col in new_columns]

    def _add_rows(self, table: str, data: any, id_column: str = 'id', column_name=''):
        """
        Entry point for adding data to the database. Accepts three types of inserting data:
        pandas.DataFrame, dict and srt-like. Dict and string data are set into the main table with
        _data_id as primary key. DataFrame leads to creating new tables with _data_id as foreign key.

        :param table: table name.
        :type table: str

        :param data: information to add.
        :type data: dict, pd.DataFrame or str-like

        :param id_column: name of primary key column
        :type id_column: str

        :param column_name: column name for adding data
        :type column_name: str-like

        """
        # noinspection PyBroadException
        try:
            column_name = preprocess_col_names(column_name)
            if isinstance(data, dict):
                cols = [column_name + '_' + preprocess_col_names(one_key) for one_key in data.keys()]
                self._create_columns(table, cols)
                data_to_sql = [None] * len(data) * 2
                data_to_sql[::2] = cols
                data_to_sql[1::2] = list(data.values())
                self._cursor.execute(str("UPDATE {} SET " + ", ".join(["{} = '{}'"] * len(data)) + " WHERE {} = '{}'").
                                     format(table, *data_to_sql, id_column, self._data_id))
            elif isinstance(data, pd.DataFrame):
                cols = [preprocess_col_names(col) for col in data.columns.to_list()]
                self._create_columns(table, cols)
                data.apply(lambda x:
                           self._cursor.execute(str("INSERT INTO {} (" +
                                                    ",".join(["{}"] * len(cols)) +
                                                    ", parent_id) values (" +
                                                    ",".join(["'{}'"] * len(cols)) +
                                                    ", '{}')").
                                                format(table, *cols, *x, self._data_id)),
                           axis=1)
            elif column_name:
                self._create_columns(table, [column_name])
                self._cursor.execute("UPDATE {} SET {} = '{}' WHERE {} = '{}'".
                                     format(table, column_name, data, id_column, self._data_id))
            self.db_connection.commit()
        except Exception as e:
            print(e)
            pass


class BasicParser:
    """
    The parent class for different parsers working with the site "zakupki.gov.ru"

    :param driver: selenium driver to load the data.
    :type driver: webdriver

    :param main_url: start page from which work starts. Can't be changed unlike a driver's page.
    :type main_url: str

    :param fast: flag for using new selenium driver for child parsers instead of the predefined one.
    :type fast: bool

    :param _id: inner id for binding with data from child parsers.
    :type _id: str

    :param _func_dict: rules for processing different data blocks from the page. Dictionary includes pairs like
    block's name and processing function.
    :type _func_dict: dict

    :param _storage: the dictionary collecting dataframes with its names.
    :type _storage: StorageAdapter

    :param _child_drivers: the list of webdrivers for child parsers.
    :type _child_drivers: list

    ..note:: The flag 'fast' needs list of webdrivers defined outside and transmitted to the class constructor.
    """
    __metaclass__ = ABCMeta

    def __init__(self, driver: webdriver,
                 storage: StorageAdapter,
                 fast: bool = False,
                 child_drivers=None):
        self.driver = driver
        self.main_url = driver.current_url
        self.fast = fast
        self._id = self._parse_id()
        self._func_dict = None
        self._child_drivers = None
        self._storage = storage
        self.set_child_drivers(child_drivers)

    @abstractmethod
    def _create_dicts(self):
        """
        Create two or more basic dictionaries.
        """
        pass

    @abstractmethod
    def _add_child_id(self):
        """Set inner id to all collected tables."""
        pass

    @abstractmethod
    def _parse_id(self):
        """Search in specified block data which can be used to identify the record"""
        pass

    def _get_child_driver(self):
        """
        Depending on 'fast' parameter return new webdriver or driver previously created.

        :return: webdriver for using in another parser
        :rtype: webdriver
        """
        if self.fast and self._child_drivers:
            return self._child_drivers[0]
        else:
            return parsing.utils.create_silent_driver()

    def _close_child_driver(self, child_driver: webdriver):
        """Depending on 'fast' parameter destroy a new webdriver."""
        if self.fast and self._child_drivers:
            pass
        else:
            child_driver.quit()

    def _get_from_href(self, parser: bs4.BeautifulSoup):
        """
         Check if parser collects a link in tag <a> and return specific data.

        :param parser: unknown tag with data
        :type parser: bs4.BeautifulSoup

        :return: link or text from tag
        :rtype: str
        """
        try:
            href = parser.find('a')
            if href is not None:
                return href['href']
            else:
                return preprocess_str(parser.text)
        except Exception as e:
            self._storage.add_error_data(self.main_url, parser, inspect.stack()[0][3], e)
            pass
            return ""

    def _get_divided_data(self, parser: bs4.BeautifulSoup):
        """
        Search data separated with sep_list's tags and union it in one string.

        :param parser: page element with data
        :type parser: bs4.BeautifulSoup

        :return: united data from the page
        :rtype: str
        """
        sep_list = ['div', 'span']
        all_text = []
        try:
            for one_sep in sep_list:
                all_elements = parser.find_all(one_sep)
                if len(all_elements) > 0:
                    all_text = all_text + [self._get_from_href(one_element) for one_element in all_elements]
            if len(all_text) == 0:
                return self._get_from_href(parser)
            else:
                return '; '.join(all_text)
        except Exception as e:
            self._storage.add_error_data(self.main_url, parser, inspect.stack()[0][3], e)
            pass
            return ""

    def _get_from_table(self, parser: tuple):
        """
        Main function for parsing data from tag <table>. If one or more cells contain another table this data will be
        reorganized to one-dimensional and put to 'spec' column.

        :param parser: pair of a table's label and a page element.
        :type parser: tuple

        :return: text or link information from tag <table>
        :rtype: pd.DataFrame

        ..note:: Some rows have 'empty' (without meaningful information) control cells, such tables always have
         empty first column. In this situation rows without empty cells pad with None values from the left side,
         otherwise all missing data adds to the right side.
        """
        parser = parser[0]
        data = None
        parsed_table = parser.find('table')
        if parsed_table is not None:
            try:
                parsed_cols = parsed_table.find('thead').find_all(['td', 'th'])
                parsed_rows = parsed_table.find('tbody').find_all('tr', recursive=False)
                columns = [preprocess_str(one_col.text) for one_col in parsed_cols]
                columns.append('additional')
                columns.append('spec')
                data = pd.DataFrame(None, columns=columns)

                for one_row in parsed_rows:
                    if one_row.find('table') is not None:
                        add_spec = self._get_from_inner_table(one_row.find('table'))
                        if add_spec is not None:
                            data.iloc[-1, -1] = np.vstack((np.array([add_spec.columns.to_numpy()]),
                                                           add_spec.to_numpy()))
                    else:
                        parsed_rows_td = one_row.find_all('td')
                        new_data = [preprocess_str(one_td.text) for one_td in parsed_rows_td]
                        delta = (len(columns) - 2) - len(parsed_rows_td)
                        if delta == 0:
                            data = data.append(pd.Series(new_data +
                                                         [''] * 2, index=columns), ignore_index=True)
                        elif 0 < delta < 3:
                            if columns[0]:
                                data = data.append(pd.Series(new_data +
                                                             [''] * delta +
                                                             [''] * 2, index=columns), ignore_index=True)
                            else:
                                data = data.append(pd.Series([''] * delta +
                                                             new_data +
                                                             [''] * 2, index=columns), ignore_index=True)
                        else:
                            data.iloc[-1, -2] = "; ".join(new_data)
            except Exception as e:
                self._storage.add_error_data(self.main_url, parser, inspect.stack()[0][3], e)
                pass
            finally:
                return data

    def _get_from_inner_table(self, parser: bs4.BeautifulSoup):
        """
        Additional function for parsing data from nested tags <table>.

        :param parser: page element collecting tag <table>
        :type parser: bs4.BeautifulSoup

        :return: table-like text or link information
        :rtype: pd.DataFrame

        ..note:: Missing data always adds to the left. Later this information will be smoothed.
        A situation when number of <tbody> columns more then <thead> didn't meet before.
        """
        data = None
        if parser is not None:
            try:
                parsed_cols = parser.find('thead').find_all(['td', 'th'])
                parsed_rows = parser.find('tbody').find_all('tr')
                columns = [preprocess_str(one_col.text) for one_col in parsed_cols]
                data = pd.DataFrame(None, columns=columns)
                for one_row in parsed_rows:
                    parsed_rows_td = one_row.find_all('td')
                    if len(parsed_rows_td) != len(columns):
                        add_col = [''] * (len(columns) - len(parsed_rows_td)) + \
                                  [preprocess_str(one_td.text) for one_td in parsed_rows_td]
                    else:
                        add_col = [preprocess_str(one_td.text) for one_td in parsed_rows_td]
                    data = data.append(pd.Series(add_col, index=columns), ignore_index=True)
            except Exception as e:
                self._storage.add_error_data(self.main_url, parser, inspect.stack()[0][3], e)
                pass
            finally:
                return data

    def _get_from_single_section(self, parser: tuple):
        """
        Collect information from one high-level data block. This type of records is separated by tags <section>
        and <span>. The first <span> tag contains a section name and the second contains an information.

        :param parser: pair of a page element with set of tags <section> and a page's label
        :type parser: tuple

        :return: dictionary collecting sections name and data
        :rtype: dict
        """
        parser = parser[0]
        all_sections = parser.find_all('section')
        result_sections = {}
        try:
            for one_section in all_sections:
                all_span = one_section.find_all('span')
                if len(all_span) == 2:
                    result_sections[preprocess_str(all_span[0].text)] = self._get_divided_data(all_span[1])
        except Exception as e:
            self._storage.add_error_data(self.main_url, parser, inspect.stack()[0][3], e)
            pass
        finally:
            return result_sections

    def _add_data_to_dataframe(self, label, parser: bs4.BeautifulSoup, n=0):
        """
        The function gains information from transferred parser and put it in dictionary.

        :param label: name of the high-level data block.
        :type label: str

        :param parser: page element with the high-level data block.
        :type parser: bs4.BeautifulSoup

        :param n: index number of the high-level data block
        :type n: int

        ..note:: First this function uses dictionary of functions _func_dict to receive information from
        webpage. Next it creates a new item of basic data into the _storage if data contains tag <table> or
        adds information to existing item named 'main'.
        """
        data = self._func_dict.get(label)((parser, label)) if self._func_dict.get(label) is not None else None
        if data is None:
            pass
        elif isinstance(data, pd.DataFrame):
            self._storage.add_table(label, n, data)
        else:
            self._storage.add_main_data(label, data)

    def get_data(self):
        return self._storage.get_data()

    def get_id(self):
        return self._id

    def set_child_drivers(self, new_drivers: list):
        self._child_drivers = new_drivers

    def parse_page(self):
        """
        Parsing entry point.
        Find basic blocks from the webpage specified by main_url,
        then turn them into BeautifulSoup parsers with specific names. Next step it mines information
        from founded HTML-blocks and records to general repository.
        """
        if self._storage.is_new:
            all_info_parsed = list(map(lambda x: bs4.BeautifulSoup(x.get_attribute('innerHTML'), 'lxml'),
                                       self.driver.find_elements_by_xpath("//div[@class='row blockInfo']/div")))
            i = 0
            for parser in all_info_parsed:
                try:
                    i += 1
                    if parser.find('h2') is not None:
                        block_name = preprocess_str(parser.find('h2').text)
                        self._add_data_to_dataframe(block_name, parser, i)
                except Exception as e:
                    self._storage.add_error_data(self.main_url, parser, inspect.stack()[0][3], e)
                    pass
            self._add_child_id()
            self._storage.roll_data()


class CustomerParser(BasicParser):
    """
    Parser for customer page.

    .. note:: Sample url: https://zakupki.gov.ru/epz/organization/view/info.html?organizationCode=xxx
    """

    def __init__(self, driver: webdriver,
                 storage: StorageAdapter,
                 fast: bool = False,
                 child_drivers=None):
        super().__init__(driver, storage, fast, child_drivers)
        self._storage.set_identifiers(self._id, 'customer')
        self._create_dicts()

    def _create_dicts(self):
        self._func_dict = {'Идентификационные коды организации (ИКО) в ЕИС': self._get_from_table,
                           'Регистрационные данные организации': self._get_from_single_section,
                           'Идентификационный код заказчика (ИКУ)': self._get_from_single_section,
                           'Форма собственности организации': self._get_from_single_section,
                           'Организационноправовая форма организации': self._get_from_single_section,
                           'Коды классификации': self._get_from_single_section,
                           'Публично-правовое образование (ППО)': self._get_from_single_section,
                           'Бюджеты': self._get_from_table,
                           'Уполномоченная организация (по Приказу Минфина России от 23.12.2014 163н)':
                               self._get_from_single_section,
                           'Уполномоченные органы, уполномоченные учреждения': self._get_from_table,
                           'Контактная информация': self._get_from_single_section}

    def _parse_id(self):
        """
        Search block named 'ИКУ'. If such section found data sets as the inner id, otherwise
        id defines by the main_url.

        :return: new inner id
        :rtype: str
        """
        # noinspection PyBroadException
        try:
            new_id = self.driver.find_element_by_xpath("//h2[text()='Идентификационный код заказчика (ИКУ)']"). \
                find_element_by_xpath('..'). \
                find_element_by_xpath("section[@class='blockInfo__section']/span[@class='section__info']")
            return re.sub('[^0-9]', '', new_id.text)
        except Exception:
            return self.main_url

    def _add_child_id(self):
        pass


class OrderParser(BasicParser):
    """
    Parser for order page. Orders are accommodated in purchasing scheduler.

    :param data_customer: parser for a customer page
    :type data_customer: CustomerParser

    .. note:: Sample url: https://zakupki.gov.ru/epz/order/notice/ea44/view/common-info.html?regNumber=xxx
    """

    def __init__(self, driver: webdriver,
                 storage: StorageAdapter,
                 fast: bool = False,
                 child_drivers=None):
        super().__init__(driver, storage, fast, child_drivers)
        self._storage.set_identifiers(self._id, 'order')
        self.data_customer = None
        self._create_dicts()

    def _parse_id(self):
        """
        Search 'MainInfo' block at the main_url page. If serial number found it sets as the inner id, otherwise
        id defines by the main_url.

        :return: new inner id
        :rtype: str
        """
        new_id = self.driver.find_elements_by_xpath("//span[@class='cardMainInfo__purchaseLink distancedText']")
        if len(new_id) > 0:
            return re.sub('[^0-9]', '', new_id[0].text)
        else:
            return self.main_url

    def _create_dicts(self):
        self._func_dict = {'Общая информация о закупке': self._create_general_section,
                           'Контактная информация': self._get_from_single_section,
                           'Информация о процедуре электронного аукциона': self._get_from_single_section,
                           'Начальная (максимальная) цена контракта': self._get_from_single_section,
                           'Информация об источниках финансирования': self._get_from_single_section,
                           'Информация об объекте закупки': self._create_trade_section,
                           'Преимущества, требования к участникам': self._get_from_single_section,
                           'Документация об электронном аукционе': self._get_from_single_section,
                           'Обеспечение заявки': self._get_from_single_section,
                           'Условия контракта': self._get_from_single_section,
                           'Обеспечение исполнения контракта': self._get_from_single_section
                           }

    def _create_general_section(self, parser: tuple):
        """
        Take information from basic block named 'Общая информация о закупке'.
        Also there is locates link to the customer page using with the customer parser data_customer.

        :param parser: page element with some HTML and its label
        :type parser: tuple

        :return: dictionary with parsed information
        :rtype: dict
        """
        new_values = self._get_from_single_section(parser)
        new_state = self.driver.find_elements_by_xpath("//span[@class='cardMainInfo__state distancedText']")
        new_values['state'] = new_state[0].text if len(new_state) > 0 else "Неизвестен"
        self.data_customer = self._create_customer_section(new_values['Размещение осуществляет'])
        return new_values

    def _create_customer_section(self, new_url: str):
        """
        Create new customer parser which collects information from organisation's page.

        :param new_url: address of a customer's page.
        :type new_url: str

        :return: filled customer parser
        :rtype: CustomerParser
        """
        cp = None
        child_driver = self._get_child_driver()
        # noinspection PyBroadException
        try:
            child_driver.get(new_url)
            cp = CustomerParser(child_driver,
                                storage=self._storage.create_child_storage(),
                                fast=self.fast)
            cp.parse_page()
        except Exception as e:
            self._storage.add_error_data(self.main_url, self.driver.current_url, inspect.stack()[0][3], e)
            pass
        finally:
            self._close_child_driver(child_driver)
            return cp

    def _create_trade_section(self, parser: tuple):
        """
        Take information from the one of basic blocks contained purchased goods. Simulates page switching if necessary.

        :param parser: tuple of a page with specific table and its label.
        :type parser: tuple

        :return: table-like information from given page
        :rtype: pd.DataFrame
        """
        label = parser[1]
        element = True
        all_data = pd.DataFrame(None)
        search_string = "//div[@id='positionKTRU' and @class='container']".format(label)
        while element is not None:
            pass
            block = self.driver.find_element_by_xpath(search_string)
            html = bs4.BeautifulSoup(block.get_attribute('innerHTML'), 'lxml')
            all_data = all_data.append(self._get_from_table((html, label)), sort=False)
            # noinspection PyBroadException
            try:
                element = self.driver.find_element_by_xpath("//a[@class='paginator-button paginator-button-next']")
                self.driver.execute_script('arguments[0].click();', element)
                wait(self.driver, 15).until(ec.staleness_of(element))
            except Exception:
                element = None
        return all_data

    def _add_child_id(self):
        """Create link between this order and the customer."""
        if self.data_customer is not None:
            self._storage.add_main_data('customer_id', self.data_customer.get_id())


class ContractParser(BasicParser):
    """
    High-level parser for a contract.

    :param data_order: parser for an order page
    :type data_order: OrderParser

    .. note:: Sample url: https://zakupki.gov.ru/epz/contract/contractCard/common-info.html?reestrNumber=xxx
    """

    def __init__(self, driver: webdriver,
                 storage: StorageAdapter,
                 fast: bool = False,
                 child_drivers=None):
        super().__init__(driver, storage, fast, child_drivers)
        self._storage.set_identifiers(self._id, 'contract')
        self.data_order = None
        self._create_dicts()
        self._parse_basics()

    def _create_dicts(self):
        self._func_dict = {'Общая информация': self._get_from_single_section,
                           'Информация о заказчике': self._get_from_single_section,
                           'Общие данные': self._get_from_single_section,
                           'Обеспечение исполнения контракта': self._get_from_single_section,
                           'Информация о поставщиках': self._get_from_table,
                           'Объекты закупки': self._create_trade_section
                           }

    def _parse_id(self):
        new_id = self.driver.find_elements_by_xpath("//span[@class='cardMainInfo__purchaseLink distancedText']")
        return re.sub('[^0-9]', '', new_id[0].text) if len(new_id) > 0 else self.main_url

    def _parse_basics(self):
        """
        Fill basic class fields.

        ..note:: Get an id from 'MainInfo' block.
        If the contract contains information about an order then this function creates new
        order parser and puts in the data_order field.
        """
        try:
            order_href = self.driver.find_element_by_xpath(
                "//a[contains(text(), '{}')]".format('Закупка')).get_attribute('href')
            driver_order = self._get_child_driver()
            driver_order.get(order_href)
            self.data_order = OrderParser(driver_order,
                                          storage=self._storage.create_child_storage(),
                                          fast=self.fast)
            if len(self._child_drivers) > 1:
                self.data_order.set_child_drivers([self._child_drivers[1]])
        except Exception as e:
            self._storage.add_error_data(self.main_url, self.driver.current_url, inspect.stack()[0][3], e)

    def _add_child_id(self):
        """
        ..note:: Create order parser and collect relevant information. If the contract has no order
        purchasing goods should be taken from the contract's second page, this requires page changing and
        repeated parsing. To avoid looping this function checks current url in selenium driver and don't parse
        the second page repeatedly.
        """
        if self.driver.current_url == self.main_url:
            self._parse_order()

    def _parse_order(self):
        """
        Collect information from an order page if it's possible.

        ..note:: If order_parser isn't empty the parser tries to collect information from its main_url and
        create the link inside main data table. If the order is unknown the second contract page opens and
        function parse_page calls repeatedly.
        """
        if self.data_order is not None:
            # noinspection PyBroadException
            try:
                self.data_order.parse_page()
                self._storage.add_main_data('order_id', self.data_order.get_id())
            except Exception as e:
                self._storage.add_error_data(self.main_url, self.driver.current_url, inspect.stack()[0][3], e)
                pass
            finally:
                self._close_child_driver(self.data_order.driver)
        else:
            try:
                self.driver.get(
                    'https://zakupki.gov.ru/epz/contract/contractCard/'
                    'payment-info-and-target-of-order.html?reestrNumber={}'.format(
                        self._id))
                self.parse_page()
            except Exception as e:
                self._storage.add_error_data(self.main_url, self.driver.current_url, inspect.stack()[0][3], e)
                pass

    def _create_trade_section(self, parser):
        """
        Take information from the one of basic blocks contained purchased goods. Simulates page switching if necessary.

        :param parser: tuple of a page with specific table and its label.
        :type parser: tuple

        :return: table-like information from given page
        :rtype: pd.DataFrame
        """
        label = parser[1]
        element = True
        all_data = pd.DataFrame(None)
        search_string = "//div[@id='contractSubjects' and @class='container']".format(label)
        while element is not None:
            pass
            block = self.driver.find_element_by_xpath(search_string)
            html = bs4.BeautifulSoup(block.get_attribute('innerHTML'), 'lxml')
            all_data = all_data.append(self._get_from_table((html,
                                                             label)), sort=False)
            # noinspection PyBroadException
            try:
                element = self.driver.find_element_by_xpath("//a[@class='paginator-button paginator-button-next']")
                self.driver.execute_script('arguments[0].click();', element)
                wait(self.driver, 15).until(ec.staleness_of(element))
            except Exception:
                element = None
        return all_data


def preprocess_str(new_str: str):
    """Collecting text should contain only letter, number and some punctuation."""
    new_str = re.sub('[^а-яА-Яa-zA-Z0-9+:/@.,(-) ]', '', str(new_str))
    new_str = re.sub('\s+', ' ', new_str)
    return new_str.strip()


def preprocess_col_names(new_str: str):
    new_str = re.sub('[^а-яА-Яa-zA-Z0-9_ ]', '', new_str)
    new_str = re.sub('\s+', '_', new_str).strip()
    return new_str if new_str else 'other'


def join_for_columns(x):
    """Auxiliary function for uniting columns in tables"""
    return ';'.join(set(x.astype(str)))

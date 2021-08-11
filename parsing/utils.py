import re
import os
import time
import sqlite3
import pathlib
import numpy as np
import pandas as pd
from math import ceil
import multiprocessing
from datetime import date
from parsing import parser
from selenium import webdriver
from selenium.webdriver.firefox.options import Options


def create_silent_driver():
    """
    Create selenium webdriver without visualization.

    :return: new webdriver
    :rtype: webdriver
    """
    options = Options()
    options.add_argument('--headless')
    return webdriver.Firefox(executable_path=r'c:\ProgramData\geckodriver.exe',
                             options=options)


def unite_two_dicts(source: dict, destination: dict, postfix: str):
    """
    Add items from first dict to second. Using the postfix to differs data from different sources with equal names.

    :param source: adding dictionary
    :type source: dict

    :param destination: receiving dictionary
    :type destination: dict

    :param postfix: additional string to distinguish different data source
    :type postfix: str

    :return: combination of two dicts
    :rtype: dict
    """
    try:
        for one_element in source.items():
            label = one_element[0] + postfix
            df = one_element[1]
            new_df = df if destination.get(label) is None else destination.get(label).append(df, sort=False)
            destination[label] = new_df
    except Exception as e:
        print(e)
    finally:
        return destination


def from_parser_to_dict(bp: parser.BasicParser, df_dict: dict):
    """
    Write parsed data to dict.
    Data from parser with the same type accumulates in one place.

    :param bp: object with parsed data
    :type bp: ps.BasicParser

    :param df_dict: dictionary with dataframes collecting parsed data
    :type df_dict: dict

    :return: inputted dictionary
    :rtype: dict
    """
    parsers_dict = {}
    if bp is None:
        return df_dict
    if isinstance(bp, parser.CustomerParser):
        parsers_dict['cup'] = bp
    elif isinstance(bp, parser.OrderParser):
        parsers_dict['op'] = bp
        if bp.data_customer is not None:
            parsers_dict['cup'] = bp.data_customer
    elif isinstance(bp, parser.ContractParser):
        parsers_dict['cop'] = bp
        if bp.data_order is not None:
            parsers_dict['op'] = bp.data_order
            if bp.data_order.data_customer is not None:
                parsers_dict['cup'] = bp.data_order.data_customer
    else:
        return df_dict
    for one_item in parsers_dict.items():
        df_dict = unite_two_dicts(one_item[1].get_data(), df_dict, "_" + one_item[0])
        # if not one_item[1].get_data()['errors'].empty:
        #     df_dict = unite_two_dicts({'errors': one_item[1].get_data()['errors']},
        #                               df_dict, "_{}_err".format(one_item[0]))
    return df_dict


def create_searching_urls(conditions: list, use_slicing=True, basic_string: str = None):
    """
    Function complements basic string with different conditions.

    :param conditions: list of dicts contained part of queries and it's values
    :type conditions: list

    :param use_slicing: flag for slicing search result for small parts
    :type use_slicing: bool

    :param basic_string: main part of full search string including "zakupki.gov.ru" and "search"
    :type basic_string: str

    :return: list of working url for searching by one condition
    :rtype: list

    ..note:: Conditions consist of special string used in site's queries and it's values.
    Slicing splits query's result for small parts to facilitate iteration through.
    Function combine one condition and time split if needed.
    """
    if basic_string is None:
        basic_string = """https://zakupki.gov.ru/epz/contract/search/results.html?fz44=on&sortBy=UPDATE_DATE"""
    sliced_dates = []
    if use_slicing:
        first_year = 2015
        last_year = date.today().year
        sliced_dates = ["publishDateFrom=01.01.{}&publishDateTo=31.03.{}".format(one_year,
                                                                                 one_year)
                        for one_year in range(first_year, last_year + 1, 1)
                        ] + ["publishDateFrom=01.04.{}&publishDateTo=30.06.{}".format(one_year,
                                                                                      one_year)
                             for one_year in range(first_year, last_year + 1, 1)
                             ] + ["publishDateFrom=01.07.{}&publishDateTo=30.09.{}".format(one_year,
                                                                                           one_year)
                                  for one_year in range(first_year, last_year + 1, 1)
                                  ] + ["publishDateFrom=01.10.{}&publishDateTo=31.12.{}".format(one_year,
                                                                                                one_year)
                                       for one_year in range(first_year, last_year + 1, 1)
                                       ]

    def prepare_string(one_element):
        one_element = list(one_element.items())[0]
        url_string = basic_string + '&' + one_element[0] + '='
        return [url_string + str(condition) for condition in one_element[1]]

    urls = np.hstack([prepare_string(one_element) for one_element in conditions])
    if use_slicing:
        urls = [one_url + '&' + one_slice for one_slice in sliced_dates for one_url in urls]
    return urls


def get_search_amount(driver: webdriver):
    """
    Get number of found records.

    :param driver: A page where function tries to find the data.
    :type driver: webdriver

    :return: number of records. If the element doesn't find returns 0.
    :rtype: int
    """
    search_result = driver.find_elements_by_xpath("//div[@class='search-results__total']")
    if search_result:
        search_result_text = re.sub('[^0-9]', '', search_result[0].text)
        return int(search_result_text) if search_result_text else 0
    else:
        return 0


def pool_url_list_to_csv(urls: list):
    """
    A parser getting order's or contract's urls from searching page. Can be used with multiprocessing.

    :param urls: list of searching urls
    :type urls: list

    :return: urls of a concrete order, contract or else
    :rtype: list
    """
    driver = create_silent_driver()
    order_urls = []
    for url in urls:
        try:
            driver.get(url)
            search_result = get_search_amount(driver)
        except Exception as e:
            print(url)
            print(e)
            search_result = 0
            pass
        i = 0
        print('Total: {}'.format(search_result))
        for page in range(1, ceil(search_result / 50) + 1):
            driver.get("{}&pageNumber={}&recordsPerPage=_50".format(url, page))
            one_page_blocks = driver.find_elements_by_xpath(
                "//div[@class='search-registry-entry-block box-shadow-search-input']")
            for one_page in one_page_blocks:
                try:
                    i += 1
                    new_url = one_page.find_element_by_class_name(
                        "registry-entry__header-mid__number").find_element_by_xpath(
                        "a").get_attribute('href')
                    order_urls.append(new_url)
                except Exception as e:
                    print(driver.current_url)
                    print(e)
                    pass
    driver.quit()
    return order_urls


def create_urls(urls_directory: str,
                conditions: list = None,
                use_slicing: bool = True,
                slow: bool = True):
    """
    Function for getting urls from the request result. Found urls write to the disk.

    :param urls_directory: the path where file is creating
    :type urls_directory: str

    :param conditions: request conditions. One condition consist of correct request string
    and it's possible options.
    :type conditions: list

    :param use_slicing: flag for splitting request result.
    :type use_slicing: bool

    :param slow: flag for multiprocessing
    :type slow: bool
    """
    if conditions is None:
        conditions = [{'ktruCodeNameList': ['21.10.60.191-00000054', '32.50.22.190-00005106',
                                            '32.50.22.190-00005106', '32.50.22.190-00005104',
                                            '21.10.60.191-00000096', '32.50.22.190-02792']},
                      {'morphology=on&searchString': ['протез', 'биопротез', 'клапан+сердца', 'протез+сосуд'
                                                                                              'перикард', 'аннулопласт',
                                                      'кондуит',
                                                      'лоскут+хирургический', 'ксенопротез+сосуда',
                                                      'заплата+сосуд', 'биопротез+клапана', 'протез+клапана',
                                                      'клапан', 'медиц', 'здрав']}]
    urls = create_searching_urls(conditions, use_slicing)
    if slow:
        order_urls = pool_url_list_to_csv(urls)
    else:
        with multiprocessing.Pool(processes=6) as pool:
            order_urls = pool.map(pool_url_list_to_csv, [urls])
    pd.DataFrame({'url': order_urls[0] if len(np.shape(order_urls)) > 1 else order_urls}).to_csv(
        os.path.join(urls_directory, 'urls.csv'), sep=";", index=False)
    print('Finished.')


def load_urls(urls_directory: str):
    """
    Loading urls from file.

    :param urls_directory: path to the directory with files
    :type urls_directory: str

    :return: read urls
    :rtype: pd.DataFrame
    """
    urls = pd.DataFrame(None, columns=['url'])
    for path in pathlib.Path(urls_directory).iterdir():
        if path.is_file():
            urls = urls.append(pd.read_csv(path, sep=';'), sort=False)
    return urls.reset_index()


# def parse_sites(parser_name, settings, storage_type: str = 'df'):
#     """
#     Function takes previously prepared list of urls and parses data from them.
#
#     :param parser_name: data parser. The parser should fits to the input urls.
#     :type parser_name: ps.BasicParser.init
#
#     :param settings: dictionary with settings including directory information
#     :type settings: dict like
#
#     :param storage_type: string with information of a storage method.
#     :type storage_type: str
#
#     """
#     start_time = time.time()
#
#     df_dict = {}
#     drivers = [create_silent_driver(), create_silent_driver(), create_silent_driver()]
#     urls = load_urls(settings.input_directory)
#     all_i = urls.shape[0]
#
#     #sqlite3.enable_shared_cache(1)
#
#     storage = parser.StorageAdapterDataframe() if storage_type == 'df' \
#         else parser.StorageAdapterSQLite(db_connection := sqlite3.connect(os.path.join(settings.db_directory,
#                                                                                        'parsing.db')),
#                                          cursor=db_connection.cursor())
#
#     for idx, url in urls.iterrows():
#         try:
#             drivers[0].get(url['url'])
#             print('{} in {}'.format(idx, all_i))
#             bp = parser_name(drivers[0],
#                              storage=storage,
#                              fast=True,
#                              child_drivers=drivers[1:])
#             bp.parse_page()
#             if storage_type == 'df':
#                 from_parser_to_dict(bp, df_dict)
#         except Exception as e:
#             print(e)
#             pass
#
#     print("--- %s seconds ---" % (time.time() - start_time))
#
#     try:
#         if storage_type == 'df':
#             for one_element in df_dict.items():
#                 one_element[1].to_csv(os.path.join(settings.output_directory,
#                                                    '{}.csv'.format(one_element[0])), sep=";", encoding='utf-8-sig')
#         print("Finished.")
#     except Exception as e:
#         raise e
#     finally:
#         storage.db_connection.close()
#         [driver.quit() for driver in drivers]
#

def parse_sites(parser_name, settings, storage_type: str = 'df'):
    """
    Function takes previously prepared list of urls and parses data from them.

    :param parser_name: data parser. The parser should fits to the input urls.
    :type parser_name: ps.BasicParser.init

    :param settings: dictionary with settings including directory information
    :type settings: dict like

    :param storage_type: string with information of a storage method.
    :type storage_type: str

    """
    start_time = time.time()
    df_dict = {}
    drivers = [create_silent_driver(), create_silent_driver(), create_silent_driver()]
    urls = load_urls(settings.input_directory)
    all_i = urls.shape[0]
    storage = parser.StorageAdapterDataframe() if storage_type == 'df' \
        else parser.StorageAdapterSQLite(db_connection := sqlite3.connect(os.path.join(settings.db_directory,
                                                                                       'parsing.db')),
                                         cursor=db_connection.cursor())

    def parse_url(inner_url):
        try:
            drivers[0].get(inner_url['url'])
            print('{} in {}'.format(inner_url['index'], all_i))
            bp = parser_name(drivers[0],
                             storage=storage,
                             fast=True,
                             child_drivers=drivers[1:])
            bp.parse_page()
            if storage_type == 'df':
                from_parser_to_dict(bp, df_dict)
        except Exception as err:
            print(err)
            pass

    urls.apply(parse_url, axis=1)

    print("--- %s seconds ---" % (time.time() - start_time))

    try:
        if storage_type == 'df':
            for one_element in df_dict.items():
                one_element[1].to_csv(os.path.join(settings.output_directory,
                                                   '{}.csv'.format(one_element[0])), sep=";", encoding='utf-8-sig')
        print("Finished.")
    except Exception as e:
        raise e
    finally:
        storage.db_connection.close()
        [driver.quit() for driver in drivers]

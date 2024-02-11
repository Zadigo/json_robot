import asyncio
import datetime
import json
import logging
import secrets
import os
from urllib.parse import urlencode

import pandas
from requests import Session
from requests.models import Request
from selenium.webdriver import Chrome, ChromeOptions, Edge, EdgeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.microsoft import EdgeChromiumDriverManager

from json_crawler import PROJECT_PATH

LOGGER = logging.getLogger('json_crawler')

CAN_CRAWL = True

RESPONSES = asyncio.Queue()

FAILED_REQUESTS = asyncio.Queue()

SELENIUM_ACTIONS = asyncio.Queue()


def get_selenium_browser_instance(browser_name='Edge', headless=False, load_images=True, load_js=True):
    """Creates a new selenium browser instance

    >>> browser = get_selenium_browser_instance()
    ... browser.get('...')
    ... browser.quit()
    """
    browser = Chrome if browser_name == 'Chrome' else Edge
    manager_instance = ChromeDriverManager if browser_name == 'Chrome' else EdgeChromiumDriverManager

    options_klass = ChromeOptions if browser_name == 'Chrome' else EdgeOptions
    options = options_klass()
    options.add_argument('--remote-allow-origins=*')
    # options.add_argument(f'--user-agent={RANDOM_USER_AGENT()}')
    options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})

    # Allow Selenium to be launched
    # in headless mode
    if headless:
        options.headless = True

    # 0 = Default, 1 = Allow, 2 = Block
    preferences = {
        'profile.default_content_setting_values': {
            'images': 0 if load_images else 2,
            'javascript': 0 if load_js else 2,
            'popups': 2,
            'geolocation': 2,
            'notifications': 2
        }
    }
    options.add_experimental_option('prefs', preferences)
    service = Service(manager_instance().install())
    return browser(service=service, options=options)


def get_current_date():
    return datetime.datetime.now()


def create_name():
    d = get_current_date().strftime('%Y-%m-%d %H:%S')
    d = d.replace(' ', '_').replace(':', '-')
    return f'file_{secrets.token_hex(5)}_{d}'


def get_current_url(current_page=1):
    root_url = 'https://recherche-entreprises.api.gouv.fr/search'
    query = {
        'page': current_page,
        'minimal': 'true',
        'per_page': 25,
        'include': 'siege',
        'activite_principale': '47.76Z'
    }
    return f'{root_url}?{urlencode(query)}'


def collect_files():
    files = PROJECT_PATH.joinpath('output').glob('**/*.json')
    df = pandas.DataFrame()
    dfs = []
    for file in files:
        f = open(file, mode='r')
        dfs.append(pandas.read_json(f))
    df = pandas.concat([df, *dfs])
    df.to_json('results.json', force_ascii=False, orient='records')


async def retries(response):
    max_retries = 3
    while CAN_CRAWL:
        while not FAILED_REQUESTS.empty():
            for _ in range(max_retries):
                print('Retry for url')
                await asyncio.sleep(10)
        await asyncio.sleep(60)


async def requester(url):
    """This sends the request to the API
    using the corresponding url"""
    session = Session()
    request = Request(method='get', url=url)
    prepared_request = session.prepare_request(request)
    prepared_request.headers.update({'content-type': 'application/json'})

    try:
        response = session.send(prepared_request)
    except Exception as e:
        return {'state': False, 'results': []}
    else:
        if response.ok:
            await RESPONSES.put(response)
            return response.json()
        await FAILED_REQUESTS.put(response)
        return {'state': False, 'results': []}


async def reader():
    """This reads the responses that were 
    stored in the Queue and then creates the
    corresponding file with the data"""
    while CAN_CRAWL:
        while not RESPONSES.empty():
            response = await RESPONSES.get()
            results = response.json().get('results', [])

            filename = f'{create_name()}.json'
            with open(PROJECT_PATH.joinpath('output', filename), mode='w', encoding='utf-8') as f:
                json.dump(results, f)
                print(f'Created file {filename}')
            await asyncio.sleep(3)
        await asyncio.sleep(30)


async def sender(task_group):
    """The main goal of this function is to organize
    the datetime at which each request should be sent"""
    global CAN_CRAWL
    print('Starting...')

    number_of_pages = None
    current_page = 1

    url = get_current_url()

    # interval = datetime.timedelta(minutes=1)
    interval = datetime.timedelta(seconds=20)
    next_execution_date = get_current_date() + interval

    # can_crawl = True
    while CAN_CRAWL:
        current_date = datetime.datetime.now()

        if current_date > next_execution_date:
            data = await task_group.create_task(requester(url))

            if number_of_pages is None:
                number_of_pages = data['total_pages']

            next_execution_date = current_date + interval
            current_page = current_page + 1
            url = get_current_url(current_page=current_page)
            print(
                f'Next execution date: {next_execution_date} for page {current_page}')
            continue

        if number_of_pages is not None:
            if current_page > number_of_pages:
                CAN_CRAWL = False
                continue

        if os.getenv('DEBUG') == 'True':
            if current_page == 2:
                CAN_CRAWL = False
                continue

        await asyncio.sleep(3)


async def navigator(driver):
    """Allows us to perform automated actions
    on the web with the data received from the API"""
    while True:
        while not SELENIUM_ACTIONS.empty():
            action = await SELENIUM_ACTIONS.get()
            driver.get('http://example.com')

        if not CAN_CRAWL and SELENIUM_ACTIONS.empty():
            break
        await asyncio.sleep(30)


async def main(use_selenium=False):
    """Main function that orchestrates the execution
    of the different tasks"""
    if use_selenium:
        driver = get_selenium_browser_instance()

    async with asyncio.TaskGroup() as t:
        t.create_task(sender(t))
        t.create_task(reader())
        
        if use_selenium:
            t.create_task(navigator(driver))


if __name__ == '__main__':
    try:
        asyncio.run(main(use_selenium=False))
    except KeyboardInterrupt:
        pass

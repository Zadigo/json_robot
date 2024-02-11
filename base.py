import asyncio
import datetime
import json
import secrets
from asyncio import Queue
from urllib.parse import urlencode

import pandas
from requests import Session
from requests.models import Request

from json_crawler import PROJECT_PATH


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


async def reader(data):
    filename = f'{create_name()}.json'
    with open(PROJECT_PATH.joinpath('output', filename), mode='w', encoding='utf-8') as f:
        json.dump(data['results'], f)
        print(f'Created file {filename}')


async def sender(url):
    session = Session()
    request = Request(method='get', url=url)
    prepared_request = session.prepare_request(request)
    prepared_request.headers = {'content-type': 'application/json'}

    try:
        response = session.send(prepared_request)
    except Exception as e:
        return {'state': False, 'results': []}
    else:
        if response.ok:
            return response.json()
        return {'state': False, 'results': []} 


async def main():
    print('Starting...')

    number_of_pages = None
    current_page = 1
    
    url = get_current_url()

    interval = datetime.timedelta(minutes=1)
    next_execution_date = get_current_date() + interval

    can_crawl = True
    while can_crawl:
        current_date = datetime.datetime.now()

        if current_date > next_execution_date:
            data = await sender(url)

            if number_of_pages is None:
                number_of_pages = data['total_pages']

            await reader(data)
            next_execution_date = current_date + interval
            current_page = current_page + 1
            url = get_current_url(current_page=current_page)
            print(f'Next execution date: {next_execution_date}')
            continue

        if number_of_pages is not None:
            if current_page > number_of_pages:
                can_crawl = False
                continue

        await asyncio.sleep(3)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

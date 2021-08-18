import asyncio
import json
import logging
import pathlib
import time
import structlog
from arsenic import services, browsers, get_session
from bs4 import BeautifulSoup
import pandas as pd

from sys import platform

if platform == "linux" or platform == "linux2":
    GECKODRIVER = './drivers/geckodriver_linux'
elif platform == "darwin":
    GECKODRIVER = './drivers/geckodrive_darwin'
elif platform == "win32":
    GECKODRIVER = './drivers/geckodriver.exe'

# GECKODRIVER = '/Users/puse/.projects/python/custom/dynamic_web_scraper/geckodriver_linux'
BASE_URL = 'https://www.crexi.com'


def set_arsenic_log_level(level=logging.WARNING):
    # Create logger
    logger = logging.getLogger('arsenic')

    # We need factory, to return application-wide logger
    def logger_factory():
        return logger

    structlog.configure(logger_factory=logger_factory)
    logger.setLevel(level)


async def get_parsable_html(body_html_str):
    # Convert raw to html tags from raw format

    return BeautifulSoup(body_html_str, 'html.parser')


async def get_broker_data(body_content):
    all_divs = body_content.find_all('div', class_='broker-info')
    datas = []
    for broker_selector in all_divs:
        broker_name = broker_selector.find('span', class_='name_text').text
        brokerage_logo = [img['src'] for img in broker_selector.findAll("img", {"class": "ng-star-inserted"})]
        data = {"broker_name": broker_name, "brokerage_logo": brokerage_logo or ""}
        datas.append(data)
    return datas


async def get_property_data(body_content):
    all_divs = body_content.find_all('crx-property-tile-aggregate', class_='ng-star-inserted')
    datas = []
    for propery_selector in all_divs:
        propery_name = propery_selector.find('div', class_='property-name').text
        property_price = propery_selector.find('div', class_='property-price').text
        property_details = propery_selector.find('div', class_='property-details').text
        property_link = [a['href'] for a in propery_selector.findAll("a", {"class": "cover-link"})]

        data = {"property-name": propery_name, "property-price": property_price, "property-details": property_details,
                "property_link": property_link[0]}

        data["property_link"] = f"{str(BASE_URL)}{str(data.get('property_link'))}"
        datas.append(data)
    return datas


async def scraper(url, context, i=-1, timeout=60, start=None):
    # service = services.Geckodriver(executable_path=GeckoDriverManager().install())
    service = services.Geckodriver(binary=GECKODRIVER)
    # Run Browser Headless
    browser = browsers.Firefox(**{'moz:firefoxOptions': {'args': ['-headless']}})
    # browser = browsers.Firefox()
    async with get_session(service, browser) as session:
        try:
            await asyncio.wait_for(session.get(url), timeout=timeout)
        except asyncio.TimeoutError:
            return []
        await asyncio.sleep(10)
        body = await session.get_page_source()  # save this locally??
        # Convert body to htmlparse
        content = await get_parsable_html(body)
        # Custom method to search for wanted files
        dataset = {}
        if context == "property":
            property_raw_data = await get_property_data(content)
            urls = []
            for j, y in enumerate(property_raw_data):
                if j < 10:
                    urls.append(y.get('property_link'))
            start = time.time()
            broker = await run(urls=urls, start=start, context="broker")
            dataset = {
                "property": property_raw_data,
                "broker": broker,
            }

            # broker = await run(urls=urls, start=start, context="broker")

        if context == "broker":
            #     '''Broker fields'''
            broker_raw_data = await get_broker_data(content)
            return broker_raw_data



        if start != None:
            end = time.time() - start
            print(f'{i} took {end} seconds')

        return dataset


async def run(urls, timeout=60, start=None, context="property"):
    results = []
    for i, url in enumerate(urls):
        results.append(
            asyncio.create_task(scraper(url, context, i=i, timeout=60, start=start))
        )
    list_of_links = await asyncio.gather(*results)
    return list_of_links


def generate_pages(url, max_pages: int):
    urls = []
    for x in range(1, max_pages + 1):
        if x == 1:
            urls.append(f"{url}")
        else:
            urls.append(f"{url}?page={x}")
    return urls


def store_links_as_df_pickle(datas: list, name='links.pkl'):
    new_df = pd.DataFrame(datas)
    og_df = pd.DataFrame([{'id': 0}])
    if pathlib.Path(name).exists():
        og_df = pd.read_pickle(name)  # read_csv
    df = pd.concat([og_df, new_df])
    df.reset_index(inplace=True, drop=False)
    df = df[['property-name', 'property-price', 'property-details', 'property_link']]
    df = df.loc[~df.id.duplicated(keep='first')]
    # df.set_index('id', inplace=True, drop=True)
    df.dropna(inplace=True)
    df.to_pickle(name)
    return df


if __name__ == "__main__":
    set_arsenic_log_level()
    start = time.time()
    url = f'{BASE_URL}/properties'
    urls = generate_pages(url=url, max_pages=1)
    name = "link.pkl"
    results = asyncio.run(run(urls, start=start))
    print(results)

    # print(json.dumps(results))
    end = time.time() - start
    print(f'total time is {end}')
    # df = store_links_as_df_pickle(results[0].get('property'), name=name)
    # print(df.head())

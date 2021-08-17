import asyncio
import json
import logging
import pathlib
import time
import structlog
from arsenic import services, browsers, get_session
from bs4 import BeautifulSoup
import pandas as pd

GECKODRIVER = './geckodriver'
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


async def get_property_data(body_content):
    all_divs = body_content.find_all('crx-property-tile-aggregate', class_='ng-star-inserted')
    datas = []
    for review_selector in all_divs:
        propery_name = review_selector.find('div', class_='property-name').text
        property_price = review_selector.find('div', class_='property-price').text
        property_details = review_selector.find('div', class_='property-details').text
        property_link = [a['href'] for a in review_selector.findAll("a", {"class": "cover-link"})]

        data = {"property-name": propery_name, "property-price": property_price, "property-details": property_details,
                "property_link": property_link[0]}

        data["property_link"] = f"{str(BASE_URL)}{str(data.get('property_link'))}"
        datas.append(data)
    return datas


async def scraper(url, i=-1, timeout=60, start=None):
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
        '''Property fields'''
        property_raw_data = await get_property_data(content)
        # product_data = await get_product_data(url, content)
        if start != None:
            end = time.time() - start
            print(f'{i} took {end} seconds')
        dataset = {
            "property": property_raw_data
            # "product_data": product_data
        }
        return dataset


async def run(urls, timeout=60, start=None):
    results = []
    for i, url in enumerate(urls):
        results.append(
            asyncio.create_task(scraper(url, i=i, timeout=60, start=start))
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
    urls = generate_pages(url=url, max_pages=10)
    name = "link.pkl"
    results = asyncio.run(run(urls, start=start))
    print(json.dumps(results))
    end = time.time() - start
    print(f'total time is {end}')
    # df = store_links_as_df_pickle(results[0].get('property'), name=name)
    # print(df.head())

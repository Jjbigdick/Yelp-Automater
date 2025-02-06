from bs4 import BeautifulSoup as bs
import time
import asyncio
import aiohttp
import Scraping_pieces_New_Old_Algorithm
import urllib.parse
import csv
from datetime import datetime
import pandas as pa

count = 0

def sort_distinct_print(csv_file_path):
    df = pa.read_csv(csv_file_path,  dtype={'phone_number': str})
    df.sort_values(by='timestamp', inplace=True)
    df.drop_duplicates(subset='name', keep='first', inplace=True)
    df.to_csv(csv_file_path, index=False)
    df = None

def decode_redirect_url(encode_url):
    encode_url = encode_url.split(";")
    for element in encode_url:
        if "redirect_url" in element:
            encoded_url = element.replace("redirect_url=","")
            decoded_url = urllib.parse.unquote(encoded_url[:-4])
            return decoded_url

def Dropduplicates_and_return_false_phone_number_tupo(csv_file_path):
    sort_distinct_print(csv_file_path)
    df = pa.read_csv(csv_file_path, dtype={'phone_number': str})
    false_tupo_index_number_list = []
    for index, row in df.iterrows():
        if row['phone_number']== "False":
            if "redirect_url" in row["profile"]:
                decoded_ulr = decode_redirect_url(row["profile"])
                false_tupo_index_number_list.append((index, decoded_ulr))
            else:
                false_tupo_index_number_list.append((index, row['profile']))
    df =None
    return false_tupo_index_number_list


async def write_to_csv_phone(index, phone):
    df.at[index, "phone_number"] = phone
    df.to_csv(csv_file_path, index=False)

def write_to_csv(csv_file_path, data):
    with open(csv_file_path, mode='a', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(data)


async def scrape2(url,headers,index):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url=url, headers=headers) as response:
                html = await response.read()
                await asyncio.sleep(0)
                html = bs(html, "html.parser")
                try:
                    target_element = html.find('p', text='Phone number')
                    phone = target_element.nextSibling.text
                    await write_to_csv_phone(index, phone)
                    global count
                    count += 1
                    logger.info(f"{phone} captured. {count}/{task_len}")
                    return 00
                except:
                    logger.critical(f"cant find element, {url} retry")
    except:
        logger.critical(f"proxy server reboot {url}")


async def scrape(tupo):
    async with semaphore:
        index = tupo[0]
        url = tupo[1]
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0',
            'Accept': 'text/html',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'utf-8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'TE': 'trailers'}
        value = await scrape2(url,headers,index)
        if value == 00:
            return


async def Gather(false_phone_number_tupo):
    tasks = []
    for tupo in false_phone_number_tupo:
        tasks.append(scrape(tupo))
    await asyncio.gather(*tasks)



if __name__ == '__main__':
    csv_file_path = "home improvement contracting businesses - Copy.csv"
    logger = Scraping_pieces_New_Old_Algorithm.logger
    timestamp = datetime.now()
    start = time.time()
    false_phone_number_tupo = Dropduplicates_and_return_false_phone_number_tupo(csv_file_path)
    df = pa.read_csv(csv_file_path)
    task_len = len(false_phone_number_tupo)
    logger.info(f"Starting collect {task_len} phone numbers.")
    semaphore = asyncio.Semaphore(40)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(Gather(false_phone_number_tupo))


    stop = time.time()
    logger.info(f"{stop - start}")














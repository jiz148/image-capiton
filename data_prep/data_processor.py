from pathlib import Path
import requests
from os import cpu_count

import datatable as dt
import asks
import trio
import pandas as pd


class PicDownloader:

    def __init__(self, data_path):
        self.data_path = data_path
        # self.i = 0

        Path('./data_images').mkdir(exist_ok=True)

        img_urls = dt.fread(data_path, sep='\t', columns={'image_url'})
        self.links = img_urls.to_list()[0][:10000]
        # self.links = img_urls.to_list()[0][:]
        print(len(self.links))

    @staticmethod
    async def fetch_pic(s, url):
        r = await s.get(url)
        return r.content

    async def save_pic(self, s, url, index):
        content = await self.fetch_pic(s, url)
        filename = f"data_images/image" + str(index) + ".png"
        with open(filename, 'wb') as f:
            f.write(content)
            # self.i += 1

    async def main(self):
        dname = 'https://upload.wikimedia.org'
        s = asks.sessions.Session(dname, connections=cpu_count() * 2)
        async with trio.open_nursery() as n:
            for index, url in enumerate(self.links):
                n.start_soon(self.save_pic, s, url, index)


if __name__ == "__main__":
    downloader = PicDownloader('data/preprocessed_data.tsv')

    trio.run(downloader.main)

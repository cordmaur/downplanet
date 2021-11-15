from pystac_client import Client
import logging
from typing import Union
from pathlib import Path
import pandas as pd
from .common import create_geometry, rm_tree
import planetary_computer as pc
import requests
from urllib.parse import urlparse
from tqdm import tqdm

catalog_url = "https://planetarycomputer.microsoft.com/api/stac/v1"
s2_collection = 'sentinel-2-l2a'


class DownPlanet:

    def __init__(self, catalog: str = catalog_url, logger_level=logging.INFO):
        """
        Create a Sentinel 2 downloader for Microsoft Planetary Computer
        :param catalog: STAC catalog to connect to. Defaults to "https://planetarycomputer.microsoft.com/api/stac/v1"
        :param logger_level: verbosity Level.
        """

        # create a logger
        logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',
                            level=logging.WARNING, datefmt='%I:%M:%S')
        self.logger = logging.getLogger('DownPlanet')
        self.logger.setLevel(logger_level)

        try:
            self.catalog = Client.open(catalog)

        except Exception as e:
            self.logger.error(f"It was not possible to open catalog: '{catalog}'.")
            self.logger.error(f'Please pass a valid STAC catalog or use the default {catalog_url}')

        self.search_df = None

    def search(self, geometry: Union[list, tuple], start_date: str, end_date: str = None):
        """
        Search for images.
        :param geometry: Polygon in the format [(long1, lat1), (long2, lat2), ...]
        If just one tuple (long, lat) is passed, it will be assumed a Point geometry.
        :param start_date:First date in the formats 'yyyy-mm-dd', 'yyyy-mm' or 'yyyy'
        :param end_date: Last date. If end_date is None, the start_date will be expanded:
        - ``2017`` expands to ``2017-01-01T00:00:00Z/2017-12-31T23:59:59Z``
        - ``2017-06`` expands to ``2017-06-01T00:00:00Z/2017-06-30T23:59:59Z``
        - ``2017-06-10`` expands to ``2017-06-10T00:00:00Z/2017-06-10T23:59:59Z``
        :return: a list of images
        """

        # create the date range
        date_range = (start_date + '/' + end_date) if end_date is not None else start_date

        # check the geometry
        aoi = create_geometry(geometry, logger=self.logger)

        search = self.catalog.search(collections=["sentinel-2-l2a"],
                                     datetime=date_range,
                                     intersects=aoi)

        # create a list with the items and save them in a data frame
        items = list(search.get_items())
        self.search_df = pd.DataFrame({item.id: item.properties for item in items}).T
        self.search_df.index.name = 'id'

        # append the item objects to the dataframe
        self.search_df['item'] = items

        self.logger.info(f'{len(self.search_df)} images found. Access .search_df for the list.')

    def download_all(self, out_dir: Union[Path, str]):
        """
        Download all the images that are in the search_df to the out_dir
        :param out_dir:
        :return:
        """


        for idx in self.search_df.index:
            self.logger.info(f'Downloading image {idx}')
            self.download(idx=idx, out_dir)



    def download(self, idx: str, out_dir: Union[Path, str]):
        """

        :param idx:
        :param out_dir:
        :return:
        """

        # check if there is a previous search
        if self.search_df is None:
            self.logger.warning(f'No search dataframe (.search_df). Do a search first.')
            return

        # check if the idx is in the search df
        if idx not in self.search_df.index:
            self.logger.warning(f'id not found in search dataframe (.search_df)')
            return

        # check if there the output directory exists
        out_dir = Path(out_dir)
        if not out_dir.exists():
            self.logger.warning(f'Output directory {str(out_dir)} does not exists. Create it first.')
            return

        # get the item to download
        item = self.search_df.loc[idx, 'item']

        # create the output folder for the image
        out_dir /= item.id + '.PC'
        if out_dir.exists():
            rm_tree(out_dir)
        out_dir.mkdir(exist_ok=True)

        # loop through the items
        for asset_name, asset in item.assets.items():
            self.logger.debug(f'Downloading asset {asset_name}')
            self.download_asset(asset, out_dir)

    def download_asset(self, asset, out_dir, pbar=False):

        href = pc.sign(asset.href)

        r = requests.get(href, stream=True)
        if not r.ok:
            self.logger.error(f'Error getting {href}')
            return
        else:
            self.logger.debug(f'Downloading asset {asset.title}')

        # create the output file name
        file_name = Path(urlparse(href).path).name
        file_path = Path(out_dir)/file_name

        # with open(file_path.as_posix(), 'wb') as f:
        #     total_length = int(r.headers.get('content-length'))
        #     for chunk in tqdm(r.iter_content(chunk_size=1024), total=(total_length/1024), unit='kb'):
        #         if chunk:
        #             f.write(chunk)
        #             f.flush()

        with open(file_path.as_posix(), 'wb') as f:
            total_length = int(r.headers.get('content-length'))
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()






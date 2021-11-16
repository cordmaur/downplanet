from pystac_client import Client
import logging
from typing import Union
from pathlib import Path
import pandas as pd
from .common import create_geometry, rm_tree, requests_retry_session
import planetary_computer as pc
import requests

from urllib.parse import urlparse
from tqdm.notebook import tqdm

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

    def download_all(self, out_dir: Union[Path, str], show_pbar=True):
        """
        Download all the images that are in the search_df to the out_dir
        :param out_dir:
        :param show_pbar:
        :return:
        """

        if show_pbar:
            iterator = tqdm(self.search_df.index, desc='All images', unit=' img')
        else:
            iterator = self.search_df.index

        for idx in iterator:
            self.logger.debug(f'Downloading image {idx}')
            self.download(idx=idx, out_dir=out_dir)

    def download(self, idx: str, out_dir: Union[Path, str]):
        """
        Download an item that is in the search_df. A directory for the specific item will be created in the
        output directory
        :param idx: index of the item to download
        :param out_dir: output directory
        :return: request response
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

        # open a session that handles retries
        session = requests_retry_session(5, status_forcelist=None)

        # Sign the item. The hrefs of the assets are updated with a token
        signed_item = self.sign_item(item, session=session)

        # Download the assets
        with tqdm(total=signed_item.size, unit_scale=True, unit='b', desc=signed_item.id, smoothing=0) as pbar:
            for asset_name, asset in signed_item.assets.items():
                self.logger.debug(f'Downloading asset {asset_name}')
                self.download_asset(asset, out_dir, session=session, pbar=pbar)

    def download_asset(self, asset, out_dir, session=None, pbar=None, sign=False):
        """
        Download an asset to the out_dir.
        :param asset: Item's asset to download (must contain .href member)
        :param out_dir: output directory
        :param session: Existing session. Otherwise, create it.
        :param pbar: if there is a progress bar, use it to update download
        :param sign: if True, sign the asset before starting the download
        :return: request response
        """

        # get the session
        session = session if session is not None else requests.Session()

        # if asset not signed, sign the asset
        href = pc.sign(asset.href) if sign else asset.href

        # open the get request in background (stream=True)
        r = session.get(href, stream=True)
        if not r.ok:
            self.logger.error(f'Error getting {href}')
            return r
        else:
            self.logger.debug(f'Downloading asset {asset.title}')

        # create the output file name
        file_name = Path(urlparse(asset.href).path).name
        file_path = Path(out_dir)/file_name

        with open(file_path.as_posix(), 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    if pbar is not None:
                        pbar.update(1024)

    @staticmethod
    def sign_item(item, session=None):
        """
        Sign all the assets in a specific item and calculate the total size.
        :param item: stac_item
        :param session: Existing session. If None, create a simple session.
        :return: item with assets' hrefs already signed and a member .size
        """

        # sign the whole item
        signed_item = pc.sign(item)

        session = session if session is not None else requests.Session()

        total_size = 0
        for asset in signed_item.assets.values():
            r = session.head(asset.href)
            total_size += int(r.headers.get('content-length'))
            r.close()

        signed_item.size = total_size

        return signed_item
from geojson import Point, Polygon
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def create_geometry(pts, logger=None):
    if isinstance(pts, tuple):
        geometry = Point(coordinates=pts)

    elif isinstance(pts, (Point, Polygon)):
        geometry = pts

    else:
        # check if the polygon is correctly closed. If it is not, close it.
        if pts[0] != pts[-1]:
            pts.append(pts[0])

        geometry = Polygon(coordinates=[pts])

    # if the geometry is not valid, return None
    if geometry.is_valid:
        return geometry
    else:
        # get the context logger
        msg = 'Informed points do not correspond to a valid polygon.'

        if logger is not None:
            logger.error(msg)
        else:
            print(msg)


# remove the folder
def rm_tree(pth):
    pth = Path(pth)
    for child in pth.glob('*'):
        if child.is_file():
            child.unlink()
        else:
            rm_tree(child)
    pth.rmdir()


# create a session that retries connecting automatically
def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

from geojson import Point, Polygon
from pathlib import Path


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
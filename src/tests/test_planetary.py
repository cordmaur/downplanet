from downplanet import DownPlanet


def test_down_planet():

    # test opening a wrong connection
    downloader = DownPlanet(catalog='xxxx')
    assert not hasattr(downloader, 'catalog')

    # test opening a good connection
    downloader = DownPlanet()
    assert hasattr(downloader, 'catalog')

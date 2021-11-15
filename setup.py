from setuptools import find_packages, setup

setup(
    name='downplanet',
    extras_require=dict(tests=['pytest']),
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        'tqdm',
        'pystac_client',
        'setuptools >= 52.0.0',
        'planetary_computer',
    ]
)


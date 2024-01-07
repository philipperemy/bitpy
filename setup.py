from setuptools import setup, find_packages

setup(
    name='bitpy',
    version='1.44',
    description='BitPy',
    author='Philippe Remy',
    install_requires=[
        'numpy',
        'pandas',
        'websocket-client',
        'requests',
        'urllib3',
        'orjson',
        'pybit'
    ],
    packages=find_packages()
)

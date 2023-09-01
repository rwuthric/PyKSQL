from setuptools import setup, find_packages
import pyksql

setup(
    name='PyKSQL',
    version=pyksql.__version__,    
    description='Python to Apache ksqlDB',
    url='https://github.com/rwuthric/PyKSQL',
    author=pyksql.__author__,
    author_email=pyksql.__email__,
    license='BSD 2-Clause License',
    packages=find_packages(),
    install_requires=['httpx[http2]', 'pandas'],
)
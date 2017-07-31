import sys
import os

from setuptools import setup, find_packages

src_path = os.path.dirname(os.path.abspath(sys.argv[0]))
old_path = os.getcwd()
os.chdir(src_path)
sys.path.insert(0, src_path)

kwargs = {}
kwargs['name'] = 'epl'
kwargs['description'] = 'Echo Park Labs GCP Imagery Library'
kwargs['long_description'] = open('README.md').read()
kwargs['author'] = 'Echo Park Labs'
kwargs['author_email'] = 'david@echoparklabs.com'
kwargs['url'] = 'https://bitbucket.org/EchoParkLabs/gcp-imagery-reader'
kwargs['version'] = '0.1dev'
kwargs['packages'] = find_packages('.')


clssfrs = [
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.3",
    "Programming Language :: Python :: 3.4",
    "Programming Language :: Python :: 3.5",
    "Programming Language :: Python :: 3.6",
]
kwargs['classifiers'] = clssfrs

setup(**kwargs)

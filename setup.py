#!/usr/bin/env python

from setuptools import setup

setup(name='tap-mailchimp',
      version='1.1.4',
      description='Singer.io tap for extracting data from the Mailchimp API',
      author='Hotglue',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_mailchimp'],
      install_requires=[
          'hotglue-singer-sdk>=1.0.31',
      ],
      entry_points='''
          [console_scripts]
          tap-mailchimp=tap_mailchimp:main
      ''',
      packages=['tap_mailchimp'],
      package_data={
          'tap_mailchimp': ['schemas/*.json'],
      }
)

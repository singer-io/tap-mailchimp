#!/usr/bin/env python

from setuptools import setup

setup(name='tap-mailchimp',
      version='1.3.1',
      description='Singer.io tap for extracting data from the Mailchimp API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_mailchimp'],
      install_requires=[
          'backoff==2.2.1',
          'requests==2.31.0',
          'singer-python==6.0.0'
      ],
      extras_require= {
          'dev': [
              'pylint==3.0.3',
              'nose2',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-mailchimp=tap_mailchimp:main
      ''',
      packages=['tap_mailchimp'],
      package_data = {
          'tap_mailchimp': ['schemas/*.json'],
      }
)

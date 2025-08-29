#!/usr/bin/env python

from setuptools import setup

setup(name='tap-mailchimp',
      version='1.3.2',
      description='Singer.io tap for extracting data from the Mailchimp API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_mailchimp'],
      install_requires=[
          'backoff==2.2.1',
          'requests==2.32.4',
          'singer-python==6.0.1'
      ],
      extras_require= {
          'dev': [
              'pylint',
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

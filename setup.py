#!/usr/bin/env python

from setuptools import setup

setup(name='tap-gitlab',
      version='0.9.0',
      description='Singer.io tap for extracting data from the GitLab API',
      author='Meltano Team && Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_gitlab'],
      install_requires=[
          'singer-python==5.6.1',
          'requests==2.20.0',
          'strict-rfc3339==0.7',
          'backoff==1.3.2'
      ],
      entry_points='''
          [console_scripts]
          tap-gitlab=tap_gitlab:main
      ''',
      packages=['tap_gitlab'],
      package_data = {
          'tap_gitlab/schemas': [
            "branches.json",
            "commits.json",
            "issues.json",
            "milestones.json",
            "projects.json",
            "users.json",
            "groups.json",
            "merge_requests.json",
            "project_members.json",
            "group_members.json",
            "project_labels.json",
            "group_labels.json",
            "tags.json",
            "releases.json",
          ],
      },
      include_package_data=True,
)

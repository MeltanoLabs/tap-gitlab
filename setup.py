from setuptools import setup

setup(
  name='tap-first',
  version='1.0',
  py_modules=['tap_first'],
  install_requires=[
    'requests',
    'singer-python',
  ],
  entry_points='''
    [console_scripts]
    tap-first=tap_first:main
  '''
)

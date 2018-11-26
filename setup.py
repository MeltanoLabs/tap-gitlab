from setuptools import setup

setup(
  name='tap-gitlab',
  version='1.0',
  py_modules=['tap_gitlab'],
  install_requires=[
    'requests',
    'singer-python',
  ],
  entry_points='''
    [console_scripts]
    tap-gitlab=tap_gitlab:main
  '''
)

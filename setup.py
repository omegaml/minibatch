import os
from pathlib import Path

from setuptools import setup, find_packages

basedir = Path(os.path.dirname(__file__))
README = open(basedir / 'README.rst').read()
version = open(basedir / 'minibatch' / 'VERSION').read()

dev_deps = ['nose', 'twine', 'flake8', 'bumpversion']
app_deps = ['flask', 'dash']
kafka_deps = ['kafka-python==1.4.7']
mqtt_deps = ['paho-mqtt==1.5.0']
mongo_deps = ['pymongo==3.10.1']
omega_deps = ['omegaml[all-client]']

setup(name='minibatch',
      version=version,
      description='Python stream processing for humans',
      url='http://github.com/omegaml/minibatch',
      long_description=README,
      long_description_content_type='text/x-rst',
      include_package_data=True,
      author='Patrick Senti',
      author_email='patrick.senti@omegaml.io',
      license='MIT',
      packages=find_packages(),
      zip_safe=False,
      install_requires=[
          # Mongo 4.2 requires at least mongoengine 0.19 due to
          # https://github.com/MongoEngine/mongoengine/pull/2160/files
          # omegaml currently only supports <0.19
          'mongoengine>=0.18,<0.19',
          'dill',
      ],
      extras_require={
          'apps': app_deps,
          'kafka': kafka_deps,
          'mqtt': mqtt_deps,
          'mongodb': mongo_deps,
          'omegaml': mongo_deps + omega_deps,
          'all': kafka_deps + mqtt_deps + mongo_deps + app_deps,
          'dev': kafka_deps + mqtt_deps + mongo_deps + omega_deps + dev_deps,
      },
      )

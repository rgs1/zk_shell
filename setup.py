import os
from setuptools import find_packages, setup
import sys


PYTHON3 = sys.version_info > (3, )
HERE = os.path.abspath(os.path.dirname(__file__))


def readme():
    with open(os.path.join(HERE, 'README.rst')) as f:
        return f.read()


def get_version():
    with open(os.path.join(HERE, "zk_shell/__init__.py"), "r") as f:
        content = "".join(f.readlines())
    env = {}
    if PYTHON3:
        exec(content, env, env)
    else:
        compiled = compile(content, "get_version", "single")
        eval(compiled, env, env)
    return env["__version__"]


setup(name='zk_shell',
      version=get_version(),
      description='A Python - Kazoo based - shell for ZooKeeper',
      long_description=readme(),
      classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        "Topic :: System :: Distributed Computing",
        "Topic :: System :: Networking",
      ],
      keywords='ZooKeeper Kazoo shell',
      url='https://github.com/rgs1/zk_shell',
      author='Raul Gutierrez Segales',
      author_email='rgs@itevenworks.net',
      license='Apache',
      packages=find_packages(),
      test_suite="zk_shell.tests",
      scripts=['bin/zk-shell'],
      install_requires=[
          'ansicolors==1.0.2',
          'kazoo==2.2.1',
          'tabulate==0.7.7',
          'twitter.common.net==0.3.9',
          'xcmd==0.0.2'
      ],
      tests_require=[
          'ansicolors==1.0.2',
          'kazoo==2.2.1',
          'nose==1.3.7',
          'tabulate==0.7.7',
          'twitter.common.net==0.3.9',
          'xcmd==0.0.2'
      ],
      extras_require={
          'test': [
              'ansicolors==1.0.2',
              'kazoo==2.2.1',
              'nose==1.3.7',
              'tabulate==0.7.7',
              'twitter.common.net==0.3.9',
              'xcmd==0.0.2'
          ]
      },
      include_package_data=True,
      zip_safe=False)

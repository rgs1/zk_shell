from setuptools import setup

def readme():
    with open('README.md') as f:
        return f.read()

setup(name='zk_shell',
      version='0.7',
      description='A Python - Kazoo based - shell for ZooKeeper',
      long_description=readme(),
      classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        "Topic :: System :: Distributed Computing",
        "Topic :: System :: Networking",
      ],
      keywords='ZooKeeper Kazoo shell',
      url='https://github.com/rgs1/zk_shell',
      author='Raul Gutierrez Segales',
      author_email='rgs@itevenworks.net',
      license='Apache',
      packages=['zk_shell'],
      scripts=['bin/zk-shell'],
      install_requires=[
          'kazoo',
      ],
      include_package_data=True,
      zip_safe=False)

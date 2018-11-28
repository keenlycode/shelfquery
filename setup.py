from setuptools import setup, find_packages


setup(
    name='shelfquery',
    version='2.0.1',
    description='ShelfDB query client for using with `shelfdb` server',
    url='https://github.com/nitipit/shelfquery',
    author='Nitipit Nontasuwan',
    author_email='nitipit@gmail.com',
    license='MIT',
    classifiers=[
        'Programming Language :: Python :: 3.7',
    ],
    keywords='shelfdb query client',
    packages=find_packages(),
    install_requires=['dill==0.2.8.2',],
)

import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='pubfisher',
    version='2019.11',
    author='Moritz Renftle',
    author_email='pubfisher@momits.de',
    description='Query information about scientific publications on the web.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/momits/pubfisher/',
    package_dir={'': 'src'},
    packages=setuptools.find_packages('src'),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    install_requires=[
        # For GTK3 frontends
        'pycairo',
        'PyGObject',

        # For web scraping
        'beautifulsoup4',

        # For BibTex analysis
        'bibtexparser',

        # For HTTP connections
        'requests',

        # For type safety
        'typeguard'
    ],
    python_requires='>=3.6',
)

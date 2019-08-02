from setuptools import setup, find_packages

with open("README.md", "r") as readme_file:
    readme = readme_file.read()

requirements = ['argparse', 'datetime', 'glob', 'json', 'logging', 'math', 'numpy', 'pandas>=0.20.0', 're', 'sqlalchemy', 'sys', 'sys', 'tabl2sql', 'time']


setup(name='tabl2sql',
      version='0.1.0',
      description='Transform tabular data to SQL',
	  long_description=readme,
      url='https://github.com/zaphodnothingth/csv2sql',
      author='Andrew Stevens',
      author_email='stevens.andrewj@gmail.com',
      license='GNU GPL V3',
      packages=find_packages(), #['tabl2sql'],
	  install_requires=requirements,
	  classifiers=[
			"Programming Language :: Python :: 3.7",
			"License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
		],
      zip_safe=False)




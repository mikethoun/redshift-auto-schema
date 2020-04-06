import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(name='redshift-auto-schema',
                 version='v0.1.14',
                 author='Mike Thoun',
                 author_email='mikethoun@gmail.com',
                 description='Auto-generate Redshift schemas',
                 long_description=long_description,
                 long_description_content_type="text/markdown",
                 url='https://github.com/mikethoun/redshift-auto-schema',
                 license='Apache License 2.0',
                 packages=setuptools.find_packages(),
                 classifiers=['Programming Language :: Python :: 3',
                              'License :: OSI Approved :: Apache Software License',
                              ],
                 install_requires=['pandas>=1.0.0',
                                   'numpy',
                                   ])

import setuptools
with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
     name='vmapcrawler',  
     version='2.0.3',
     author="Vu Viet Hung",
     author_email="vuviethung.98.hust@gmail.com",
     description="Crawler for Vmap map",
     url="https://github.com/vuviethung1998/vmapcrawler",
     long_description=long_description,
     long_description_content_type="text/markdown",
     py_modules=["vmapcrawler"],
     packages=setuptools.find_packages(),
     classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
     ],
 )
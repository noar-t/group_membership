import setuptools

NAME = "membership"
DESCRIPTION = "Membership Protocol"
VERSION = "0.1"
AUTHOR = "Will and Noah"
AUTHOR_EMAIL = "wlsaidhi@utexas.edu"


setuptools.setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    packages=setuptools.find_packages(exclude=['tests']),
)

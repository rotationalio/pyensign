"""
Python SDK package for the Ensign platform.
"""

##########################################################################
## Module Info
##########################################################################

# Import the version at the top level
from .version import __version_info__, get_version

##########################################################################
## Package Version
##########################################################################

__version__ = get_version()

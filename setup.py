__author__ = 'LMai'
from distutils.core import setup
import py2exe

# setup(console=['etf_upload.py'],
#       options={"py2exe": {"includes": "decimal"}})

setup(
    options={'py2exe': {'bundle_files': 1, 'compressed': True, 'includes': 'decimal'}
                        ,'packages': ['elementtree', 'xml']
             },
    console=[{'script': "etf_upload.py"}],
    zipfile=None,
)

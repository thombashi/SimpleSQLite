Installation
============
Install from PyPI
------------------------------
::

    pip install SimpleSQLite

Install from PPA (for Ubuntu)
------------------------------
::

    sudo add-apt-repository ppa:thombashi/ppa
    sudo apt update
    sudo apt install python3-simplesqlite


Dependencies
============
Python 2.7+ or 3.5+

Mandatory Dependencies
----------------------------------
- `DataProperty <https://github.com/thombashi/DataProperty>`__ (Used to extract data types)
- `mbstrdecoder <https://github.com/thombashi/mbstrdecoder>`__
- `pathvalidate <https://github.com/thombashi/pathvalidate>`__
- `six <https://pypi.org/project/six/>`__
- `sqliteschema <https://github.com/thombashi/sqliteschema>`__
- `tabledata <https://github.com/thombashi/tabledata>`__
- `typepy <https://github.com/thombashi/typepy>`__

Optional Dependencies
----------------------------------
- `loguru <https://github.com/Delgan/loguru>`__
    - Used for logging if the package installed
- `pandas <https://pandas.pydata.org/>`__
- `pytablereader <https://github.com/thombashi/pytablereader>`__

Test Dependencies
----------------------------------
- `pytest <https://docs.pytest.org/en/latest/>`__
- `pytest-runner <https://github.com/pytest-dev/pytest-runner>`__
- `tox <https://testrun.org/tox/latest/>`__

# 6.5830_final_project

## Loading raw data from the TPC-H benchmark
1. Unzip `tpch_raw_data.zip` (shared via Dropbox link:
https://www.dropbox.com/scl/fi/undzcd2pmks2krc061dia/tpch_raw_data.zip?rlkey=3dkn5jvv3modyd5uyu3t06i90&st=cqty46mp&dl=0)
2. Move file contents to a directory named `tpch_raw_data` inside the root
directory.
3. In the root directory, run `python3 setup_database.py`.
4. Verify a new file `tpch.db` is created, and the script outputs the commented
query result.

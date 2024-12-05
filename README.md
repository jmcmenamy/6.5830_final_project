# 6.5830_final_project

## Loading raw data from the TPC-H benchmark

1. Unzip `tpch_raw_data.zip` (shared via Dropbox link:
   https://www.dropbox.com/scl/fi/undzcd2pmks2krc061dia/tpch_raw_data.zip?rlkey=3dkn5jvv3modyd5uyu3t06i90&st=cqty46mp&dl=0)
2. Move file contents to a directory named `tpch_raw_data` inside the root
   directory.
3. In the root directory, run `python3 setup_database.py`.
4. Verify a new file `tpch.db` is created, and the script outputs the commented
   query result.

## Using raw data in godb terminal

1. navigate to `go-db-2024` directory
2. run `go run main.go`
3. run `\i ../tpch_raw_data/catalog.txt`
4. This will by default only load some of the data, jumping to random offsets in
   the file on each query. The description in main.go describes how to change the
   arguments to change the behavior.

- `\i ../tpch_raw_data/catalog.txt false false All` will load all the data
- `\i ../tpch_raw_data/catalog.txt false false Stat` will load a randomly sampled
  subset of the data, while ensuring all loaded rows containing non-outlier values
  based on precomputed per-column statistics.
- `\i ../tpch_raw_data/catalog.txt false` will read some of the data each time,
  but not use a metadata file to keep track of offsets that have been read
- `\i ../tpch_raw_data/catalog.txt false true Contiguous` will read some of the data each time, contiguously iterating through the file,
  keep a stat file with the statistics for the subset of data it has read
- `\i ../tpch_raw_data/catalog.txt true true Stratified` will read some of the data each time by randomly seeking to a point in the file, then reading contiguously from there.

## Running Godb and removing metadata files between runs

- `./rundb.sh "\i ../tpch_raw_data/catalog.txt"` will remove all .dat, Stat.txt, and Info.txt files in tpch_raw_data/ between runs. This makes it easier to do many trials. Just change the command within the quotes for the first input you want to put into godb. If you get an error running the script, do `chmod +x rundb.sh`.
- `./wrapdb.sh "\i ../tpch_raw_data/catalog.txt"` will enter a loop that repeatedly takes a user input for a command to run in godb. Once it has this input, it will run the equivalent of rundb.sh, restarting godb and remoting all the .dat, Stat.txt, and Info.txt files, and then running the user provided command. This can be helpful to run fresh trials for different queries quickly. If you get an error running the script, do `chmod +x wrapdb.sh`.

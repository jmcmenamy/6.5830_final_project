#!/bin/bash

# Check if the input string was provided
if [ -z "$1" ]; then
  echo "Error: No input string provided."
  exit 1
fi

# Assign the first argument to the input_string variable
input_string="$1"

# Run the rm command to remove specific files
rm ../tpch_raw_data/*Info.txt ../tpch_raw_data/*Stat.txt ../tpch_raw_data/*.dat

# Create a named pipe (FIFO)
fifo_name="/tmp/go_input_pipe"
rm "$fifo_name"
mkfifo "$fifo_name"

# Run the Go program in the background, with its input coming from the FIFO
go run main.go < "$fifo_name" &

# Get the PID of the Go process to ensure it's running
go_pid=$!

# Send the initial input string to the named pipe
echo "$input_string" > "$fifo_name"

# Now that the Go program has received its first input, allow manual input
echo "You can now interact with the Go program manually. Type your input below:"
cat < /dev/tty > "$fifo_name"  # Redirect terminal input to the FIFO

# Wait for the Go process to finish (optional)
wait $go_pid

# Clean up by removing the FIFO after the program finishes
rm "$fifo_name"


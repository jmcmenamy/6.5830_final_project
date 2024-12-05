#!/bin/bash

# Check if the initial input string is provided
if [ -z "$1" ]; then
  echo "Error: No input string provided."
  exit 1
fi

# Assign the first argument to the input_string variable
input_string="$1"
echo "input is $input_string"
# go_pid=1

# Function to kill the Go process and clean up
kill_go_process() {
    # Check if the Go process is running
    if [ -n "$go_pid" ] && ps -p $go_pid > /dev/null 2>&1; then
        echo "Killing the existing go process..."
        kill $go_pid
        wait $go_pid 2>/dev/null
    fi
}

# Function to start the Go program
start_go_program() {
    # Run the rm command to remove specific files
    rm ../tpch_raw_data/*Info.txt ../tpch_raw_data/*Stat.txt ../tpch_raw_data/*.dat

    # Create a named pipe (FIFO)
    fifo_name="/tmp/go_input_pipe"
    mkfifo "$fifo_name"

    # Run the Go program in the background, with its input coming from the FIFO
    go run main.go < "$fifo_name" &

    # Get the PID of the Go process to ensure it's running
    go_pid=$!

    # Send the initial input string to the named pipe
    echo "$input_string" > "$fifo_name"
    echo "$user_input" > "$fifo_name"
}

# Main loop
while true; do
    # Start the Go program and send the initial input
    # Prompt for a new input string
    echo "Enter the next input string (or type 'exit' to quit). Input is $input_string: "
    # Initialize an empty variable to store multiline input
    user_input=""

    # Loop to read each line and append it to the variable
    while IFS= read -r line; do
        # Stop reading when an empty line is entered
        # [[ -z "$line" ]] && break
        user_input+="$line"$'\n'
        if [[ "$line" == *\; ]] || [[ "$line" == \\* ]]; then
            break
        fi
    done
    # echo "User input was $user_input" 
    if [ "$user_input" == "exit" ]; then
        break
    fi
    # echo "LOOK go_pid is $go_pid"
    # ps
    kill_go_process
    start_go_program

    # Now that the Go program has received its first input, allow manual input
    # echo "You can now interact with the Go program manually. Type your input below:"
    
    # # Read one line of input from the user and send it to the FIFO
    # read user_input
    # echo "READ INPUT"
    # echo "$input_string" > "/tmp/go_input_pipe"  # Send the input to the Go program
    # echo "GOT HEREEE"

    # After the user input, kill the Go process and restart the loop
    # kill_go_process
done

#!/bin/bash

# Print header
echo "======================================"
echo "Running M3 Tests"
echo "======================================"
echo

# Clean up existing directories
echo "Cleaning up existing directories..."
rm -rf ECS165
echo "Cleanup completed"
echo

# Function to run a test with proper formatting
run_test() {
    echo "Running $1..."
    echo "--------------------------------------"
    python "$1"
    echo "--------------------------------------"
    echo "$1 completed"
    echo
}

# # Run M3 Part 1 Test (Concurrent Operations)
# run_test m3_tester_part_1.py

# # Run M3 Part 2 Test (Data Persistence with Concurrency)
# run_test m3_tester_part_2.py

# # Clean up directories again
# echo "Cleaning up directories for exam tests..."
# rm -rf Lineage_DB ECS165
# echo "Cleanup completed"
# echo

# Run Exam M3 Part 1 Test
run_test exam_tester_m3_part1.py

# Run Exam M3 Part 2 Test
run_test exam_tester_m3_part2.py

# Print footer
echo "======================================"
echo "All M3 tests completed"
echo "======================================" 
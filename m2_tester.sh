#!/bin/bash

# Print header
echo "======================================"
echo "Running M2 Tests"
echo "======================================"
echo

# Clean up existing directories
echo "Cleaning up existing directories..."
rm -rf Lineage_DB ECS165
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

# Run M2 Part 1 Test
run_test m2_tester_part1.py

# Run M2 Part 2 Test
run_test m2_tester_part2.py

# Clean up directories again
echo "Cleaning up directories for exam tests..."
rm -rf Lineage_DB ECS165
echo "Cleanup completed"
echo

# Run Exam M2 Part 1 Test
run_test exam_tester_m2_part1.py

# Run Exam M2 Part 2 Test
run_test exam_tester_m2_part2.py

# Print footer
echo "======================================"
echo "All M2 tests completed"
echo "======================================"

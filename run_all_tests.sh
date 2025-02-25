#!/bin/bash

# Print header
echo "======================================"
echo "Running All Tests"
echo "======================================"
echo

# Function to run a test file with proper formatting
run_test() {
    echo "Running $1..."
    echo "--------------------------------------"
    python "$1"
    echo "--------------------------------------"
    echo "$1 completed"
    echo
}

# M1 Tests
echo "======================================"
echo "Running M1 Tests"
echo "======================================"
echo
run_test m1_tester.py
run_test exam_tester_m1.py

# M2 Tests
echo "======================================"
echo "Running M2 Tests"
echo "======================================"
echo
run_test m2_tester_part1.py
run_test m2_tester_part2.py
run_test exam_tester_m2_part1.py
run_test exam_tester_m2_part2.py

# M3 Tests
echo "======================================"
echo "Running M3 Tests"
echo "======================================"
echo
run_test m3_tester_part_1.py
run_test m3_tester_part_2.py
run_test exam_tester_m3_part1.py
run_test exam_tester_m3_part2.py

# Print footer
echo "======================================"
echo "All tests completed"
echo "======================================" 
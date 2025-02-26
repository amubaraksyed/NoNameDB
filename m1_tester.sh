#!/bin/bash

# Print header
echo "======================================"
echo "Running M1 Tests"
echo "======================================"
echo

# Clean up existing directories
echo "Cleaning up existing directories..."
rm -rf Lineage_DB ECS165
echo "Cleanup completed"
echo

# Run m1_tester.py
echo "Running m1_tester.py..."
echo "--------------------------------------"
python m1_tester.py
echo "--------------------------------------"
echo "m1_tester.py completed"
echo

# Print separator
echo "======================================"
echo

# Run exam_tester_m1.py
echo "Running exam_tester_m1.py..."
echo "--------------------------------------"
python exam_tester_m1.py
echo "--------------------------------------"
echo "exam_tester_m1.py completed"
echo

# Print footer
echo "======================================"
echo "All tests completed"
echo "======================================" 
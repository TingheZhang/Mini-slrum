#!/bin/bash
#SBATCH --job-name=test_cancel
#SBATCH --cpus-per-task=1
#SBATCH --gres=gpu:1
#SBATCH --time=00:05:00

echo "Cancel test running on $(hostname)"
sleep 120
echo "Cancel test done"

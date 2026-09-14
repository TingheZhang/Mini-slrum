#!/bin/bash
#SBATCH --job-name=live_test
#SBATCH --cpus-per-task=1
#SBATCH --gres=gpu:1
#SBATCH --time=00:02:00

echo "=== Hostname ==="
hostname
echo "=== Date ==="
date
echo "=== CUDA_VISIBLE_DEVICES ==="
echo "$CUDA_VISIBLE_DEVICES"
echo "=== GPU Query ==="
nvidia-smi --query-gpu=index,name,memory.total --format=csv,noheader
echo "=== JOB DONE ==="

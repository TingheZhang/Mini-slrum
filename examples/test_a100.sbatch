#!/bin/bash
#SBATCH --job-name=test_a100
#SBATCH --cpus-per-task=2
#SBATCH --gres=gpu:1
#SBATCH -C a100
#SBATCH --time=00:02:00

echo "Node: $(hostname)"
echo "CUDA_VISIBLE_DEVICES: $CUDA_VISIBLE_DEVICES"
nvidia-smi --query-gpu=index,name,memory.total --format=csv,noheader
echo "SUCCESS_A100"

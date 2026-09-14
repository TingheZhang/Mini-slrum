#!/bin/bash
#SBATCH --job-name=live_test
#SBATCH --cpus-per-task=1
#SBATCH --gres=gpu:1
#SBATCH --time=00:02:00

echo " Running on \HCCR-93KD8V3 at \Mon Sep 14 09:54:23 EDT 2026\
echo \CUDA_VISIBLE_DEVICES=\\
nvidia-smi --query-gpu=index,name,memory.total --format=csv,noheader
echo \TEST JOB COMPLETED SUCCESSFULLY\

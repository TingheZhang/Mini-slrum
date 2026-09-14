#!/bin/bash
#SBATCH --job-name=test_cpu
#SBATCH --cpus-per-task=4
#SBATCH --mem=2000
#SBATCH --time=00:01:00

echo "Pure CPU task on $(hostname)"
python3 -c "import math; print('CPU Calculation result:', sum(math.sin(x) for x in range(1000000)))"
echo "CPU_DONE"

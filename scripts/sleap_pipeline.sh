#!/bin/bash

# usage: when transferring from plugcameras 50, 51, and 52 for example, use the following:
# sbatch --export=EXP_NAME=test_exp,RIG_NUMBERS="50 51 52",IP_FILE=ip_addresses.csv,PIPELINE=2 pipeline.sh

#SBATCH --job-name=slp-pipe
#SBATCH --ntasks=1
#SBATCH --time=12:00:00
#SBATCH --mem=16G
#SBATCH --partition=ncpu
#SBATCH --cpus-per-task=8
#SBATCH --output=slurm-%j.out
#SBATCH --mail-user=$(whoami)@crick.ac.uk
#SBATCH --mail-type=FAIL

ml purge
ml Anaconda3/2023.09-0
source /camp/apps/eb/software/Anaconda/conda.env.sh

conda activate sleap

# Initialize command with the part that always needs to be executed
cmd="python sleap_pipeline.py -p "$SAVE_PATH" -v "$VIDEO_PATH" -m1 "$CENTROID" -m2 "$CEN_INS" -s "$PARTS""

# Execute the command and redirect output to log file
eval $cmd > python_output.log 2>&1
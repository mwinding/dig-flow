#!/bin/bash

# usage: when transferring from plugcameras 50, 51, and 52 for example, use the following:
# sbatch --export=EXP_NAME=test_exp,RIG_NUMBERS="50 51 52",IP_FILE=ip_addresses.csv,PIPELINE=2 pipeline.sh

#SBATCH --job-name=SLEAP_infer
#SBATCH --ntasks=1
#SBATCH --time=08:00:00
#SBATCH --mem=32G
#SBATCH --partition=cpu
#SBATCH --cpus-per-task=8
#SBATCH --output=slurm-%j.out
#SBATCH --mail-user=$(whoami)@crick.ac.uk
#SBATCH --mail-type=FAIL

ml purge
ml Anaconda3/2023.09-0
ml FFmpeg/4.1-foss-2018b
source /camp/apps/eb/software/Anaconda/conda.env.sh

conda activate pyimagej-env

# Initialize command with the part that always needs to be executed
cmd="python plugcamera_pipeline.py -e "$EXP_NAME" -p "$PIPELINE""

# Check if RIG_NUMBERS is set and not empty
if [[ -n "$RIG_NUMBERS" ]]; then
    # Convert RIG_NUMBERS into an array
    IFS=' ' read -r -a rig_numbers_array <<< "$RIG_NUMBERS"
    # Append to the command
    cmd+=" -l ${rig_numbers_array[@]}"
fi

# Check if IP_FILE is set and not empty
if [[ -n "$IP_FILE" ]]; then
    # Append to the command
    cmd+=" -ip "$IP_FILE""
fi

# Execute the command and redirect output to log file
eval $cmd > python_output.log 2>&1
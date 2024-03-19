import subprocess
import pandas as pd
from datetime import datetime
import os
import argparse
import time
import tempfile

class Experiment:
    def __init__(self, experiment_name, rig_list=None, ip_path='ip_addresses.csv', exp_type='plugcamera', remove_files=True):
        # *** add information about ip_addresses.csv format ***
        # *** add general information ***

        self.name = experiment_name
        self.exp_type = exp_type
        self.rig_list = rig_list
        self.ip_path = ip_path
        self.remove_files = '--remove-source-files' if remove_files else ''

        self.ip_data = None
        self.IPs = None
        self.rig_num = None
        self.save_path = None
        self.video_path = None
        self.raw_data_path = None
        self.mp4_path = None
        self.rpi_username = None

        self.load_ip_data()
        self.process_ips_of_interest()
        self.setup_experiment_paths()

    ########################
    # initialisation methods
    ########################
    def load_ip_data(self):
        self.ip_data = pd.read_csv(self.ip_path)
        self.IPs = self.ip_data.IP_address
        self.rig_num = self.ip_data.rig_number

    def process_ips_of_interest(self): # select only IP addresses corresponding to rig_list, if rig_list is provided
        if self.rig_list:
            self.ip_data.index = self.rig_num
            self.IPs = self.ip_data.loc[self.rig_list, 'IP_address'].values
            self.rig_num = self.rig_list

    def setup_experiment_paths(self):
        if self.exp_type == 'plugcamera':
            self.rpi_username = self.exp_type
            self.save_path = f'/camp/lab/windingm/data/instruments/behavioural_rigs/{self.exp_type}/{self.name}'
            self.video_path = f'/home/{self.rpi_username}/data/'
            self.raw_data_path = f'{self.save_path}/raw_data'
            self.mp4_path = f'{self.save_path}/mp4s'

    ###################################################
    # PIPELINES: Transfer and process data
    ###################################################

    # for plugcamera
    def pc_pipeline1():
        transfer_data() # transfers data from individual RPis to NEMO .../data/instruments/behavioural_rigs/...
        crop_mp4_convert() # converts .jpgs to .mp4 and crops to smaller size

    def pc_pipeline2():
        return

    def make_dir(path):
        if not os.path.exists(path):
            os.makdirs(path, exist_ok=True)
        
        return(path)

    def set_start_time(track_type):
        if(track_type=='transfer'): self.transfer_start_time = datetime.now()
        elif(track_type=='process'): self.process_start_time = datetime.now()

    def set_end_time(track_type):
        if(track_type=='transfer'): self.transfer_end_time = datetime.now()
        elif(track_type=='process'): self.process_end_time = datetime.now()
        

    def transfer_data(self):
        self.set_start_time('transfer')

        shell_script_content = self.sbatch_scripts('array_transfer')

        job_id = shell_script_run(shell_script_content)
        check_job_completed(job_id)

        self.set_end_time('transfer')

    def shell_script_run(shell_script_content):
        # Create a temporary file to hold the SBATCH script
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp_script:
            tmp_script.write(shell_script_content)
            tmp_script_path = tmp_script.name

        # Submit the SBATCH script
        process = subprocess.run(["sbatch", tmp_script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Optionally, delete the temporary file after submission
        os.unlink(tmp_script_path)

        # Check the result and extract job ID from the output
        if process.returncode == 0:
            job_id_output = process.stdout.strip()
            print(job_id_output)

            job_id = job_id_output.split()[-1]

            print(process.stdout)
            return(job_id)

        else:
            print("Failed to submit job")
            print(process.stderr)
            exit(1)

    # Function to check if the array job is completed
    def is_job_completed(job_id):
        cmd = ["sacct", "-j", f"{job_id}", "--format=JobID,State", "--noheader"]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, text=True)
        lines = result.stdout.strip().split('\n')

        # Initialize flags
        all_completed = True

        for line in lines:
            parts = line.split()
            if len(parts) < 2:
                continue  # Skip any malformed lines

            # Extract and ignore array job steps (e.g., "12345678_1") for simplicity
            job_id_part, job_state = parts[0], parts[1]
            if "_" in job_id_part and not job_id_part.endswith("batch"):  # Focus on array tasks excluding batch job steps
                if job_state not in ["COMPLETED", "FAILED", "CANCELLED"]:
                    all_completed = False
                    break

        return all_completed

    def check_job_completed(job_id, initial_wait=60, wait=30):
        seconds = initial_wait
        print(f"Wait for {seconds} seconds before checking if slurm job has completed")
        time.sleep(seconds)
        
        # Wait for the array job to complete
        print(f"Waiting for slurm job {job_id} to complete...")
        while not is_job_array_completed(job_id):
            print(f"Slurm job {job_id} is still running. Waiting...")
            time.sleep(wait)  # Check every 30 seconds

        print(f"Slurm job {job_id} has completed.\n")


    # generate and crop mp4 videos for each directory
    def run_commands_in_directory(directory_path, save_path):
        # Define the commands
        generate_mp4 = f"ffmpeg -framerate 7 -pattern_type glob -i '{directory_path}/*.jpg' -c:v libx264 -pix_fmt yuv420p {directory_path}_raw.mp4"
        crop_mp4 = f"ffmpeg -i {directory_path}_raw.mp4 -filter:v 'crop=1750:1750:1430:360' {save_path}.mp4"
        remove_uncropped = f"rm {directory_path}_raw.mp4"

        # Run the commands using subprocess
        subprocess.run(generate_mp4, shell=True)
        subprocess.run(crop_mp4, shell=True)
        subprocess.run(remove_uncropped, shell=True)

    def list_directory_contents(folder_path):
        # Check if the given path is a directory
        if not os.path.isdir(folder_path):
            print(f"{folder_path} is not a valid directory path.")
            return
        
        # Get the list of items in the directory
        return os.listdir(folder_path)
        
    def crop_mp4_convert():
        self.set_start_time('process')

        base_path = self.raw_data_path
        save_path = self.mp4_path
        
        # Path to the parent directory with the folders you want to list
        directory_contents = self.list_directory_contents(base_path)

        if directory_contents:
            print(f"Processing each directory in {base_path}:")
            for directory in directory_contents:
                print(f"\nProcessing: {base_path}/{directory}")
                self.run_commands_in_directory(f'{base_path}/{directory}', f'{save_path}/{directory}')
        else:
            print("No directories found.")

        self.set_end_time('process')

    # collection of sbatch scripts for pipelines
    def sbatch_scripts(script_type):

        # for array job transfer of plugcamera data from RPis directly to NEMO
        if(script_type=='array_transfer'):
            IPs_string = ' '.join(self.IPs)

            script = f"""#!/bin/bash
            #SBATCH --job-name=rsync_pis
            #SBATCH --ntasks=1
            #SBATCH --cpus-per-task=4
            #SBATCH --array=1-{len(self.IPs)}
            #SBATCH --partition=cpu
            #SBATCH --mem=10G
            #SBATCH --time=08:00:00

            # convert ip_string to shell array
            IFS=' ' read -r -a ip_array <<< "{IPs_string}"
            ip_var="${{ip_array[$SLURM_ARRAY_TASK_ID-1]}}"

            # rsync using the IP address obtained above

            echo $ip_var

            rsync -avzh --progress {self.remove_files}{self.username}@$ip_var:{self.video_path} {self.raw_data_path}
            rsync_status=$?

            # check rsync status and output file if it fails to allow user to easily notice
            if [ $rsync_status -ne 0 ]; then
                # If rsync fails, create a file indicating failure
                echo "Rsync failed for IP: $ip_var" > "FAILED-rsync_IP-$ip_var.out"
            fi

            ssh {self.username}@$ip_var "find data/ -mindepth 1 -type d -empty -delete"
            """

        return script
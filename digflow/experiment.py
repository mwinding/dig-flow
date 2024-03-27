import subprocess
import pandas as pd
import numpy as np
from datetime import datetime
import os
import argparse
import time
import tempfile
from PIL import Image
import shutil
import scyjava
import cv2
import imagej
import random
import json

class Experiment:
    def __init__(self, experiment_name, exp_type, conditions=None, rig_list=None, ip_path='ip_addresses.csv', remove_files=True):
        # *** add information about ip_addresses.csv format ***
        # *** add general information ***

        self.name = experiment_name
        #self.conditions = conditions[0]
        #self.N = self.set_N(conditions[1])
        self.rig_list = rig_list
        self.remove_files = '--remove-source-files ' if remove_files else ''
        self.centroid_path = '/camp/lab/windingm/home/shared/SLEAP_models/pupae_detection/240306_235934.centroid'
        self.centered_instance_path = '/camp/lab/windingm/home/shared/SLEAP_models/pupae_detection/240306_235934.centered_instance'
        self.fiji_path = '/camp/lab/windingm/home/shared/Fiji-installation/Fiji.app'
        self.ip_path = ip_path
        self.exp_type = exp_type

        self.ip_data = None
        self.IPs = None
        self.rig_num = None
        self.save_path = None
        self.save_path_pupae = None
        self.video_path = None
        self.raw_data_path = None
        self.mp4_path = None
        self.rpi_username = None
        self.predictions_path = None
        self.transfer_start_time = None
        self.transfer_end_time = None
        self.process_start_time = None
        self.process_end_time = None

        self.load_ip_data()
        self.process_ips_of_interest()
        self.generate_experiment_csv()

        random.seed(time.time()) # seeds random module with current time to ensure that the random seed is never the same

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

    # parse N_input, aka conditions[1], properly
    def set_N(self, N_input):
        if len(N_input)>1: return N_input
        else: return N_input * len(self.conditions)

    def generate_experiment_csv(self):
        # 1-12, 13-24, 25-36, 37-48, 49-60, 61-72
        # Shelf 1-3

        positions_shelf_1 = [np.arange(1, 12),
                                np.arange(13, 24),
                                np.arange(25, 36),
                                np.arange(37, 48),
                                np.arange(49, 60),
                                np.arange(61, 72)]

        positions_shelf_2 = positions_shelf_1.copy()
        positions_shelf_3 = positions_shelf_1.copy()

        # filter out used positions
        # filter_positions()
        
            # how to deal with N>12 experiments? put them all sequential or select random rows throughout the incubator

        # potential start positions
        all_positions = positions_shelf_1 + positions_shelf_2 + positions_shelf_3
        starts = [min(x) for x in all_positions]

        start_position = random.choice(starts)

        # fill up positions for whole experiment
        # identify_all_positions()

        # make sure script can handle changes in position

    def setup_experiment_paths(self, exp):
        if exp == 'plugcamera':
            self.rpi_username = 'plugcamera'
            self.save_path = f'/camp/lab/windingm/data/instruments/behavioural_rigs/{self.exp_type}/{self.name}'
            self.video_path = f'/home/{self.rpi_username}/data/'

            self.raw_data_path = f'{self.save_path}/raw_data'
            self.mp4_path = f'{self.save_path}/mp4s'

            for folder in [self.save_path, self.raw_data_path, self.mp4_path]:
                os.makedirs(folder, exist_ok=True)

        elif exp == 'pupae':
            self.rpi_username = 'plugcamera' #'rotator' CHANGED FOR TESTING PURPOSES
            self.IPs = '10.7.192.115' # for testing
            self.save_path = f'/camp/lab/windingm/data/instruments/behavioural_rigs/{self.exp_type}/{self.name}/pupae'
            self.video_path = f'/home/{self.rpi_username}/data/'

            self.raw_data_path = f'{self.save_path}/raw_data'
            self.predictions_path = f'{self.save_path}/predictions'

            print('\nPATHS...')
            print(f"\tsave_path is {self.save_path}")
            print(f"\tvideo_path is {self.video_path}")
            print(f"\traw_data_path is {self.raw_data_path}")
            print(f"\tpredictions_path is {self.predictions_path}")

            for folder in [self.save_path, self.raw_data_path, self.predictions_path]:
                os.makedirs(folder, exist_ok=True)

    ###################################################
    # PIPELINES: Transfer and process data
    ###################################################

    # for plugcamera
    def pc_pipeline1(self):
        self.setup_experiment_paths('plugcamera')
        self.transfer_data('array_transfer') # transfers data from individual RPis to NEMO
        self.crop_mp4_convert() # converts .jpgs to .mp4 and crops to smaller size
        self.timing()

    def pc_pipeline2(self):
        # exp_csv = pd.read_csv(experiment_csv_path)
        self.setup_experiment_paths('pupae')
        self.transfer_data('pupae_transfer')    # transfers data from rotator RPis to NEMO

        scyjava.config.add_option('-Xmx6g')
        self.ij = imagej.init(self.fiji_path)   # point to local installation
        self.unwrap_videos()                    # unwraps rotating vial videos
        #self.timing()

    def pc_pipeline3(self):
        self.setup_experiment_paths('pupae')
        self.sleap_prediction()                 # infers pupae locations using pretrained SLEAP model
        #self.write_predictions()                # 
        #self.timing()

    ##########
    # METHODS
    ##########
    def make_dir(self, path):
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        return(path)

    def set_start_time(self, track_type):
        if(track_type=='transfer'): self.transfer_start_time = datetime.now()
        elif(track_type=='process'): self.process_start_time = datetime.now()

    def set_end_time(self, track_type):
        if(track_type=='transfer'): self.transfer_end_time = datetime.now()
        elif(track_type=='process'): self.process_end_time = datetime.now()
        
    def set_username(self, user): self.rpi_username = user
    def set_fiji_path(self, fiji_path): self.fiji_path = fiji_path
    def set_centroid_path(self, centroid_path): self.centroid_path = centroid_path
    def set_centered_instance_path(self, centered_instance_path): self.centered_instance_path = centered_instance_path

    def transfer_data(self, script_type):
        self.set_start_time('transfer')
        print('\nData Transfer from RPis to NEMO...\n')

        shell_script_content = self.sbatch_scripts(script_type)

        job_id = self.shell_script_run(shell_script_content)
        self.check_job_completed(job_id)

        self.set_end_time('transfer')

    def sleap_prediction(self):

        print('\nSLEAP predictions of pupae locations...\n')
        script_content = self.sbatch_scripts('sleap_predict')
        job_id = self.shell_script_run(script_content)
        self.check_job_completed(job_id)

        self.set_end_time('process')

    def shell_script_run(self, shell_script_content):
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
    def is_job_completed(self, job_id):
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

    def check_job_completed(self, job_id, initial_wait=10, wait=30):
        seconds = initial_wait
        print(f"Wait for {seconds} seconds before checking if slurm job has completed")
        time.sleep(seconds)
        
        # Wait for the array job to complete
        print(f"Waiting for slurm job {job_id} to complete...")
        while not self.is_job_completed(job_id):
            print(f"Slurm job {job_id} is still running. Waiting...")
            time.sleep(wait)  # Check every 30 seconds

        print(f"Slurm job {job_id} has completed.\n")


    # generate and crop mp4 videos for each directory
    def run_commands_in_directory(self, directory_path, save_path):
        # Define the commands
        generate_mp4 = f"ffmpeg -framerate 7 -pattern_type glob -i '{directory_path}/*.jpg' -c:v libx264 -pix_fmt yuv420p {directory_path}_raw.mp4"
        crop_mp4 = f"ffmpeg -i {directory_path}_raw.mp4 -filter:v 'crop=1750:1750:1430:360' {save_path}.mp4"
        remove_uncropped = f"rm {directory_path}_raw.mp4"

        # Run the commands using subprocess
        subprocess.run(generate_mp4, shell=True)
        subprocess.run(crop_mp4, shell=True)
        subprocess.run(remove_uncropped, shell=True)

    def list_directory_contents(self, folder_path):
        # Check if the given path is a directory
        if not os.path.isdir(folder_path):
            print(f"{folder_path} is not a valid directory path.")
            return
        
        # Get the list of items in the directory
        return os.listdir(folder_path)
        
    def crop_mp4_convert(self):
        self.set_start_time('process')
        print('\nConverting .jpgs to .mp4...\n')

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

        
    # extract frames from video and crop centre 150 pixels
    def extract_frames(self, video_path, interval=1, save_path='', crop=[525, 675], stop_frame = 250): #250
        """
        Extract frames from a video.
        
        :param video_path: Path to the video file.
        :param interval: Interval of frames to extract (1 = every frame, 2 = every other frame, etc.)
        """

        vidcap = cv2.VideoCapture(video_path)
        success, image = vidcap.read()
        count = 0
        frames = []

        while success:
            if count <= stop_frame and (count % interval == 0):  # Save frame every 'interval' frames
                frames.append(image)
            success, image = vidcap.read()
            count += 1

        vidcap.release()
    
        os.makedirs(f'{save_path}/sequence/', exist_ok=True)
        for i, frame in enumerate(frames):
            frame = frame[:, crop[0]:crop[1]]
            cv2.imwrite(f'{save_path}/sequence/{str(i).zfill(3)}.jpg', frame)

        return(frames)

    def stitch_images(self, frames, save_path, name, tile_config=None):
        path = f'{self.raw_data_path}/sequence'

        print(f'Stitching {len(frames)} frames together...')

        plugin = "Grid/Collection stitching"
        args = {
            "type": "[Grid: row-by-row]",
            "order": "[Right & Down]",
            "grid_size_x": f"{(len(frames))}",
            "grid_size_y": "1",
            "tile_overlap": "86",
            "first_file_index_i": "0",
            "directory": f'{path}',
            "file_names": "{iii}.jpg",
            "output_textfile_name": "TileConfiguration.txt",
            "fusion_method": "[Linear Blending]",
            "regression_threshold": "0.30",
            "max/avg_displacement_threshold": "2.50",
            "absolute_displacement_threshold": "3.50",
            "compute_overlap": True,
            "computation_parameters": "[Save computation time (but use more RAM)]",
            "image_output": "[Write to disk]",
            "output_directory": f'{path}'
        }

        # if tile_config=True, stitch based on tile configuration file
        if(tile_config!=None):

            self.get_tile_config()

            args = {
                "type": "[Positions from file]",
                "order": "[Defined by TileConfiguration]",
                "layout_file": 'TileConfiguration.txt',
                "directory": f'{path}',
                "fusion_method": "[Linear Blending]",
                "regression_threshold": "0.30",
                "max/avg_displacement_threshold": "2.50",
                "absolute_displacement_threshold": "3.50",
                "compute_overlap": True,
                "computation_parameters": "[Save computation time (but use more RAM)]",
                "image_output": "[Write to disk]",
                "output_directory": f'{path}'
            }

        # run plugin
        self.ij.py.run_plugin(plugin, args)

        # Fiji stitcher saves output as separate 8-bit R, G, and B images
        # merge them together and save here

        # Open the 8-bit grayscale TIFF images
        image_r = Image.open(f'{path}/img_t1_z1_c1')
        image_g = Image.open(f'{path}/img_t1_z1_c2')
        image_b = Image.open(f'{path}/img_t1_z1_c3')

        # Merge the images into one RGB image
        image_rgb = Image.merge('RGB', (image_r, image_g, image_b))

        # Define crop box with left, upper, right, and lower coordinates; heuristically defined by looking at uncropped images
        crop_box = (0, 0, 1045, image_rgb.height)  # Right coordinate is 1050, lower coordinate is the height of the image

        # Crop the image
        cropped_image_rgb = image_rgb.crop(crop_box)

        # save the image
        cropped_image_rgb.save(f'{self.raw_data_path}/{name}.jpg')
        
        # delete everything from sequence directory and then directory itself
        try:
            shutil.rmtree(f'{path}/')
        except:
            print('Cannot delete folder!')

        return(f'{self.raw_data_path}/{name}.jpg')

    def unwrap_videos(self, tile_config=True):
        self.set_start_time('process')

        video_path = self.raw_data_path

        # batch process videos in folder
        paths = []
        names = []
        if(os.path.isdir(video_path)):
            video_files = [f'{video_path}/{f}' for f in os.listdir(video_path) if os.path.isfile(os.path.join(video_path, f)) and not (f.endswith('.txt') or f=='.DS_Store')]

            for video_file_path in video_files:
                frames = self.extract_frames(video_file_path, interval=5, save_path=video_path)
                name = os.path.basename(video_file_path)
                path = self.stitch_images(frames=frames, save_path=video_path, tile_config=tile_config, name=name)

                names.append(name) # return file name for subsequent saving
                paths.append(path) # return all paths of unwrapped videos for subsequent processing

            self.remove_tile_config(f"{self.raw_data_path}/sequence/TileConfiguration.txt") # delete tile configuration after using Fiji plugin

        return paths, names

    # *** NEED TO IMPLEMENT DIRECT WRITING TO CSV ***
    def write_predictions(self):
        counts = []
        if(os.path.isdir(self.predictions_path)):
            video_files = [f'{self.predictions_path}/{f}' for f in os.listdir(self.predictions_path) if os.path.isfile(os.path.join(self.predictions_path, f)) and not (f.endswith('.slp') or f=='.DS_Store')]
            for video_file in video_files:
                with open(video_file, 'r') as file:
                    data = json.load(file)

                    pupae_count = len(data['labels'][0]['_instances'])
                    print([pupae_count, video_file])
                    counts.append([pupae_count, video_file])

        df = pd.DataFrame(counts, columns = ['pupae_count', 'dataset'])
        df.to_csv(f'{self.predictions_path}/pupae_counts.csv')

        # body_x, body_y = data['labels'][0]['_instances'][0]['_points']['0']['x'], data['labels'][0]['_instances'][0]['_points']['0']['y']
        # tail_x, tail_y = data['labels'][0]['_instances'][0]['_points']['1']['x'], data['labels'][0]['_instances'][0]['_points']['1']['y']
        # head_x, head_y = data['labels'][0]['_instances'][0]['_points']['2']['x'], data['labels'][0]['_instances'][0]['_points']['2']['y']

        self.set_end_time('processing')

    def timing(self):
        # calculate and print how long pipeline took

        rsync_time = self.transfer_end_time - self.transfer_start_time
        processing_time = self.process_end_time - self.process_start_time
        total_time = self.process_end_time - self.transfer_start_time

        # Convert duration to total seconds for formatting
        rsync_seconds = int(rsync_time.total_seconds())
        processing_seconds = int(processing_time.total_seconds())
        total_seconds = int(total_time.total_seconds())

        # Format durations as MM:SS
        rsync_time_formatted = f'{rsync_seconds // 60}:{rsync_seconds % 60:02d}'
        processing_time_formatted = f'{processing_seconds // 60}:{processing_seconds % 60:02d}'
        total_time_formatted = f'{total_seconds // 60}:{total_seconds % 60:02d}'


        print('\n\n\n')
        print(f'Rsync time: {rsync_time_formatted}')
        print(f'Processing time: {processing_time_formatted}')
        print(f'\nTotal time: {total_time_formatted}')

    # collection of sbatch scripts for pipelines
    def sbatch_scripts(self, script_type):

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
            #SBATCH --mail-user=$(whoami)@crick.ac.uk
            #SBATCH --mail-type=FAIL

            # convert ip_string to shell array
            IFS=' ' read -r -a ip_array <<< "{IPs_string}"
            ip_var="${{ip_array[$SLURM_ARRAY_TASK_ID-1]}}"

            # rsync using the IP address obtained above

            echo $ip_var

            rsync -avzh --progress {self.remove_files}{self.rpi_username}@$ip_var:{self.video_path} {self.raw_data_path}
            rsync_status=$?

            # check rsync status and output file if it fails to allow user to easily notice
            if [ $rsync_status -ne 0 ]; then
                # If rsync fails, create a file indicating failure
                echo "Rsync failed for IP: $ip_var" > "FAILED-rsync_IP-$ip_var.out"
            fi

            ssh {self.rpi_username}@$ip_var "find data/ -mindepth 1 -type d -empty -delete"
            """

        if(script_type=='pupae_transfer'):

            script = f"""#!/bin/bash
            #SBATCH --job-name=rsync_pis
            #SBATCH --ntasks=1
            #SBATCH --cpus-per-task=4
            #SBATCH --partition=cpu
            #SBATCH --mem=10G
            #SBATCH --time=08:00:00
            #SBATCH --mail-user=$(whoami)@crick.ac.uk
            #SBATCH --mail-type=FAIL

            # rsync using the IP address obtained above
            rsync -avzh --progress {self.remove_files}{self.rpi_username}@{self.IPs}:{self.video_path} {self.raw_data_path}
            rsync_status=$?

            # check rsync status and output file if it fails to allow user to easily notice
            echo "Rsync failed for IP: $ip_var" > "FAILED-rsync_IP-$ip_var.out"
            """
    
        if(script_type=='sleap_predict'):

            script = f"""#!/bin/bash
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
            source /camp/apps/eb/software/Anaconda/conda.env.sh

            conda activate sleap

            for video in {self.raw_data_path}/*.jpg
            do
                name_var=$(basename "$video" .jpg)
                sleap-track "$video" -m {self.centroid_path} -m {self.centered_instance_path} -o {self.predictions_path}/$name_var.predictions.slp
                sleap-convert {self.predictions_path}/$name_var.predictions.slp -o {self.predictions_path}/$name_var.json --format json
            done"""

        return script

    def get_tile_config(self):

        file_name = f"{self.raw_data_path}/sequence/TileConfiguration.txt"

        content = """# Define the number of dimensions we are working on
        dim = 2

        # Define the image coordinates
        000.jpg; ; (0.0, 0.0)
        001.jpg; ; (21.0, 0.0)
        002.jpg; ; (42.0, 0.0)
        003.jpg; ; (63.0, 0.0)
        004.jpg; ; (84.0, 0.0)
        005.jpg; ; (105.0, 0.0)
        006.jpg; ; (126.0, 0.0)
        007.jpg; ; (147.0, 0.0)
        008.jpg; ; (168.0, 0.0)
        009.jpg; ; (189.0, 0.0)
        010.jpg; ; (210.0, 0.0)
        011.jpg; ; (231.0, 0.0)
        012.jpg; ; (252.0, 0.0)
        013.jpg; ; (273.0, 0.0)
        014.jpg; ; (294.0, 0.0)
        015.jpg; ; (315.0, 0.0)
        016.jpg; ; (336.0, 0.0)
        017.jpg; ; (357.0, 0.0)
        018.jpg; ; (378.0, 0.0)
        019.jpg; ; (399.0, 0.0)
        020.jpg; ; (420.0, 0.0)
        021.jpg; ; (441.0, 0.0)
        022.jpg; ; (462.0, 0.0)
        023.jpg; ; (483.0, 0.0)
        024.jpg; ; (504.0, 0.0)
        025.jpg; ; (525.0, 0.0)
        026.jpg; ; (546.0, 0.0)
        027.jpg; ; (567.0, 0.0)
        028.jpg; ; (588.0, 0.0)
        029.jpg; ; (609.0, 0.0)
        030.jpg; ; (630.0, 0.0)
        031.jpg; ; (651.0, 0.0)
        032.jpg; ; (672.0, 0.0)
        033.jpg; ; (693.0, 0.0)
        034.jpg; ; (714.0, 0.0)
        035.jpg; ; (735.0, 0.0)
        036.jpg; ; (756.0, 0.0)
        037.jpg; ; (777.0, 0.0)
        038.jpg; ; (798.0, 0.0)
        039.jpg; ; (819.0, 0.0)
        040.jpg; ; (840.0, 0.0)
        041.jpg; ; (861.0, 0.0)
        042.jpg; ; (882.0, 0.0)
        043.jpg; ; (903.0, 0.0)
        044.jpg; ; (924.0, 0.0)
        045.jpg; ; (945.0, 0.0)
        046.jpg; ; (966.0, 0.0)
        047.jpg; ; (987.0, 0.0)
        048.jpg; ; (1008.0, 0.0)
        049.jpg; ; (1029.0, 0.0)
        050.jpg; ; (1050.0, 0.0)"""

        # Write the content to the file
        with open(file_name, "w") as file:
            file.write(content)

        return file_name

    def remove_tile_config(self, file_path):
        # Check if the file exists to avoid FileNotFoundError, delete if present
        if os.path.exists(file_path):
            os.remove(file_path)
        else:
            print(f"The file {file_path} does not exist.")

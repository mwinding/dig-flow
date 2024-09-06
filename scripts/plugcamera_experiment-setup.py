import pandas as pd
import numpy as np
import digflow as dig
import argparse

# pulling user-input variables from command line
parser = argparse.ArgumentParser(description='plugcamera pipeline: transferring data from RPis to NEMO, initial processing')
parser.add_argument('-f', '--first-run', dest='first_run', action='store', type=str, default='False', help='True/False whether it is the first time running the script')
parser.add_argument('-p', '--file_path', dest='file_path', action='store', type=str, help='path to the save file from last session')
parser.add_argument('-d', '--wc-date', dest='wc_date', action='store', type=str, required=True, help='Mondays date for week of collections')
parser.add_argument('-s', '--save-path', dest='save_path', action='store', type=str, default=None, help='path to output folder')
parser.add_argument('-n', '--sample-size', dest='sample_size', action='store', type=str, default=None, help='number of samples required per condition, must be divisible by 3')
parser.add_argument('-c', '--conditions', dest='conditions', action='store', type=str, default=None, help='conditions to be tested, recommend to be in the tray-position format from the fly stock database')
parser.add_argument('-e', '--experimenters', dest='experimenters', action='store', type=str, nargs='+', default=['Lucy', 'Lena', 'Alice', 'Anna', 'Michael'], help='names of experimenters')
parser.add_argument('-cn', '--control_sample_size', dest='control_sample_size', action='store', type=str, help='number of controls per collection')

# ingesting user-input arguments
args = parser.parse_args()
first_run = args.first_run
file_path = args.file_path
wc_date = args.wc_date
save_path = args.save_path
sample_size = int(args.sample_size)
conditions = args.conditions
experimenters = args.experimenters
control_sample_size = int(args.control_sample_size)

if first_run=='True':
    design = dig.Design(wc_date=wc_date, save_path=save_path,sample_size=sample_size, conditions=conditions, experimenters=experimenters, controls_per_collection=control_sample_size)
    design.vials_gui()
    design.output()

if first_run=='False':
    design = dig.Design(wc_date=wc_date, file=file_path)
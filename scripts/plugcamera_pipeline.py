import digflow as dig
import argparse

# pulling user-input variables from command line
parser = argparse.ArgumentParser(description='plugcamera pipeline: transferring data from RPis to NEMO, initial processing')
parser.add_argument('-e', '--experiment-name', dest='experiment_name', action='store', type=str, required=True, help='location of video file')
parser.add_argument('-l', '--rig-list', nargs='+', type=int, default=None, help='list of rig names')
parser.add_argument('-ip', '--ip-path', dest='ip_path', action='store', type=str, required=True)
parser.add_argument('-p', '--pipeline', dest='pipeline', action='store', type=int, required=True)

# ingesting user-input arguments
args = parser.parse_args()
experiment_name = args.experiment_name
rig_list = args.rig_list
ip_path = args.ip_path
pipeline = args.pipeline

exp = dig.Experiment(experiment_name=experiment_name, exp_type='plugcamera', rig_list=rig_list, ip_path=ip_path)

if(pipeline==1): exp.pc_pipeline1()
if(pipeline==2): exp.pc_pipeline2()
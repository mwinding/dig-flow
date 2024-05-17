import digflow as dig
import argparse

# pulling user-input variables from command line
parser = argparse.ArgumentParser(description='plugcamera pipeline: transferring data from RPis to NEMO, initial processing')
parser.add_argument('-p', '--predictions-path', dest='predictions_path', action='store', type=str, required=True, help='path to save folder for predictions')
parser.add_argument('-v', '--video-path', dest='video_path', action='store', type=str, default=None, help='path to folder with video(s)')
parser.add_argument('-m1', '--centroid-path', dest='centroid_path', action='store', type=str, default=None, help='path to centroid model')
parser.add_argument('-m2', '--centered-instance-path', dest='centered_instance_path', action='store', type=str, default=None, help='path to centered instance model')
parser.add_argument('-s', '--skel_parts', dest='skel_parts', action='store', type=str, default=None, help='skeleton parts separated by spaces')


# ingesting user-input arguments
args = parser.parse_args()
predictions_path = args.predictions_path
video_path = args.video_path
centroid_path = args.centroid_path
centered_instance_path = args.centered_instance_path
skel_parts = args.skel_parts

sleap_paths = [predictions_path,
                video_path,
                centroid_path,
                centered_instance_path]

exp = dig.Experiment(exp_type='sleap', sleap_paths=sleap_paths, skel_parts=skel_parts)
exp.sleap_pipeline1()
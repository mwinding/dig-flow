import pandas as pd
import numpy as np
import os
import argparse
import random
import json
from datetime import datetime, timedelta

class Screen:
    def __init__(self, wc_date, save_path=None, conditions=None, conditions_df=None, sample_size=None, controls_per_collection=None, folder=None):

        self.save_path = save_path
        if folder==None: self.conditions = list(pd.read_csv(conditions, header=0).conditions)

        if sample_size!=None:
            if sample_size % 6 == 0:
                self.sample_size = int(sample_size / 6) # each experiment has 6 replicates, there are collections on Tuesday evening, Wednesday, Wednesday evening, Thursday, Thursday evening, Friday
            else: raise ValueError(f"Sample_size must be divisible by 6\nsample_size:{sample_size} is not divisible by 6")

        self.folder = folder
        self.week_spreadsheet = None
        self.master_spreadsheet = None
        self.amendment = None
        self.all_exps = None
        self.completed_exps = []
        self.remaining_exps = None
        self.shelves = []
        self.shelves_df = pd.DataFrame()
        if conditions_df!=None: self.conditions_df =  pd.read_csv(conditions_df, header=0)
        self.condition_locations = {}
        self.controls_per_collection = controls_per_collection
        self.date = wc_date
        self.check_if_monday(wc_date)
        self.wc_date = wc_date # Monday's date (to calculate when Tuesday and Wednesday collections happened)

        # work in progress
        if folder!=None:
            with open(f'{folder}/experiment.json', 'r') as f:
                json_data = json.load(f)
            self.conditions = json_data['conditions']
            self.remaining_exps = json_data['remaining']
            self.condition_locations = json_data['condition_locations']
            self.completed_exps = json_data['completed']
            self.controls_per_collection = json_data['controls_per_collection']

            # find amendments, e.g. failed experiments and add back to remaining_exps
            self.amendment = pd.read_csv(f'{folder}/shelves.csv', index_col=0)

            # Check if there are any amendments with value -1
            amend_bool = self.amendment.amendments == -1

            if amend_bool.any():  # Proceed only if there are -1 entries
                failed_exp = self.amendment[amend_bool].condition.values

                # Create a copy of original lists to avoid modifying it while iterating
                exp_list = self.completed_exps.copy()
                remaining_exp_list = self.remaining_exps.copy()

                # Iterate over the failed_exp list and remove elements from exp_list
                # and add back elements to remaining_exp_list
                for item in failed_exp:
                    # Check if the item exists in exp_list before removing it
                    if item in exp_list:
                        index = len(exp_list) - 1 - exp_list[::-1].index(item)
                        exp_list.pop(index)

                        # Insert item at random index in remaining_exp_list
                        index = random.randint(0, len(remaining_exp_list))
                        remaining_exp_list.insert(index, item)

                self.completed_exps = exp_list
                self.remaining_exps = remaining_exp_list
            else:
                print("No amendments with value -1 found. Skipping amendment processing.")

        if folder==None:
            self.conditions_init(seed=42)
            self.first_experiment = True

    def conditions_init(self, seed):
        random.seed(seed)
        random.shuffle(self.conditions)
        self.all_exps = self.conditions * self.sample_size # add repeats after the conditions are shuffled, so they only repeat after each condition occurs once, twice, etc.
        self.remaining_exps = self.all_exps
        # note that self.all_exps and self.remaining_exps refer to sets of experiments. Here experiment has by default 6 replicates

        self.condition_locations = self.link_conditions_with_locations(self.conditions, self.conditions_df)

    def link_conditions_with_locations(self, conditions: list[str], stock_df: pd.DataFrame) -> dict[str, str]:
        """
        Given a list of condition names and a stock DataFrame with
        columns 'condition' and 'location', return a dict mapping
        each condition to its stock location.
        """

        stock_df['condition'] = stock_df['ID']
        stock_df['location'] = stock_df['Tray'].astype(str) + '-' + stock_df['Location'].astype(str)

        # Keep only the relevant columns
        stock_df = stock_df[['condition', 'location']]

        # Create a DataFrame from your conditions
        exp_df = pd.DataFrame({'condition': conditions})

        # Merge them — keeps all experiment conditions even if not in stock_df
        merged = exp_df.merge(stock_df, on='condition', how='left')

        # Convert to dict: condition -> location (may contain NaN if not found)
        return dict(zip(merged['condition'], merged['location']))

    def check_if_monday(self, wc_date):
        monday_date = datetime.strptime(wc_date, "%d-%m-%Y")
        
        # Check if the date is a Monday (weekday() returns 0 for Monday)
        if monday_date.weekday() != 0:
            raise ValueError(f"The provided date, wc_date:{wc_date}, is not a Monday. Please use this format: DD-MM-YYYY")

    def calculate_dates(self, date_type):
        # Convert the Monday date string to a datetime object
        monday_date = datetime.strptime(self.wc_date, "%d-%m-%Y")

        def format_date_range(date1: datetime, date2: datetime) -> str:
            """Return a formatted date range like '14-15/10/2025'."""
            # Ensure date1 is before date2
            if date2 < date1:
                date1, date2 = date2, date1

            # If same month and year, use compact format
            if date1.month == date2.month and date1.year == date2.year:
                return f"{date1.day}-{date2.day:02d}/{date1.month:02d}/{date1.year}"
            # Otherwise, use full dates
            else:
                return f"{date1.strftime('%d/%m/%Y')}-{date2.strftime('%d/%m/%Y')}"

        if date_type=='staging': # meaning when larvae are staged for experiments

            tues = monday_date + timedelta(days=1)
            wed = monday_date + timedelta(days=2)
            thurs = monday_date + timedelta(days=3)
            fri = monday_date + timedelta(days=4)

            tues_night = format_date_range(tues, wed)
            wed_night = format_date_range(wed, thurs)
            thurs_night = format_date_range(thurs, fri)

            # Return the dates as strings in the 'DD/MM/YYYY' format or 'DD-DD/MM/YYYY' for night ranges
            return tues_night, wed.strftime("%d/%m/%Y"), wed_night, thurs.strftime("%d/%m/%Y"), thurs_night, fri.strftime("%d/%m/%Y")

        if data_type=='expansion': # meanning when males are expanded for crosses
            #wed = monday_date + timedelta(days=2)
            #thurs = monday_date + timedelta(days=3)
            #fri = monday_date + timedelta(days=4)

            return #wed.strftime("%d-%m-%Y"), thurs.strftime("%d-%m-%Y"), fri.strftime("%d-%m-%Y")

    def select_conditions(self, num_conditions=None):
        if num_conditions is None:
            num_conditions = 24 - self.controls_per_collection

        if len(self.remaining_exps) < num_conditions:
            raise ValueError("Not enough remaining experiments to select the requested number of conditions.")
        
        selected_conditions = self.remaining_exps[:num_conditions]
        self.remaining_exps = self.remaining_exps[num_conditions:]
        self.completed_exps.extend(selected_conditions)

        return selected_conditions

    def build_shelves(self):
        df = pd.DataFrame(columns=[
            'experimenter','collector','incubator','shelf','rack','plugcamera',
            'condition','location','staging_date','amendments','comments','staging_times'
        ])
        dates = self.calculate_dates('staging')  # [Tue-night, Wed, Wed-night, Thu, Thu-night, Fri]

        # Select the 23 conditions ONCE per incubator
        inc_conds = {
            1: self.select_conditions(),
            2: self.select_conditions(),
        }

        for idx, date in enumerate(dates):
            # shelf/rack assignment per date
            is_range = '-' in date          # e.g. "15-16/10/2024" => True
            shelf_num = 2 if is_range else 1
            rack_num = (idx // 2) + 1

            # Emit incubator 1 first, then incubator 2 for THIS date
            for incubator in (1, 2):
                shelf_conditions = inc_conds[incubator].copy()
                # add controls
                for _ in range(self.controls_per_collection):
                    shelf_conditions.append('control')
                random.shuffle(shelf_conditions)

                # rows
                for condition in shelf_conditions:
                    location = self.condition_locations.get(condition, '')
                    df = pd.concat([df, pd.DataFrame({
                        'experimenter': [''],
                        'collector': [''],
                        'incubator': [incubator],
                        'shelf': [shelf_num],
                        'rack': [rack_num],
                        'plugcamera': [''],
                        'condition': [condition],
                        'location': [location],
                        'staging_date': [date],
                        'amendments': [''],
                        'comments': [''],
                        'staging_times': ['']
                    })], ignore_index=True)

        return df


    def run(self):
        save_path = f'{self.save_path}/{self.date}'
        os.makedirs(save_path, exist_ok=True)

        # save df from build_shelves to csv
        shelves_df = self.build_shelves()
        shelves_df.to_csv(f'{save_path}/shelves.csv')

        # Save the experiment JSON
        experiment_dict = {'conditions': self.conditions,
                            'remaining': self.remaining_exps,
                            'completed': self.completed_exps,
                            'controls_per_collection': self.controls_per_collection,
                            'condition_locations': self.condition_locations}

        with open(f'{save_path}/experiment.json', 'w') as f:
            json.dump(experiment_dict, f, indent=4)


# pulling user-input variables from command line
parser = argparse.ArgumentParser(description='set up initial spreadsheet for inactivation screen')
parser.add_argument('-f', '--file-path', dest='file_path', type=str, required=True,help='path to save folder for inactivation conditions')
parser.add_argument('-d', '--date', dest='wc_date', action='store', type=str, required=True, help='date of the Monday when the week starts, format: DD-MM-YYYY')
parser.add_argument('-s', '--sample-size', dest='sample_size', action='store', type=int, required=True, help='number of times each condition is repeated (must be divisible by 6)')
parser.add_argument('-c', '--controls-per-collection', dest='controls_per_collection', action='store', type=int, required=True, help='number of control experiments to include per collection day')
parser.add_argument('--folder', dest='folder', action='store', type=str, required=False, help='path to existing folder to continue an experiment')
parser.add_argument('--conditions-df', dest='conditions_df', action='store', type=str, required=False, help='path to conditions locations csv file, only needed if starting a new experiment')
parser.add_argument('--conditions', dest='conditions', type=str, required=True, help='path to conditions csv file, only needed if starting a new experiment')

args = parser.parse_args()

# run the Screen setup
screen = Screen(wc_date=args.wc_date,
                save_path=args.file_path,
                conditions=args.conditions,
                conditions_df=args.conditions_df,
                sample_size=args.sample_size,
                controls_per_collection=args.controls_per_collection)

screen.run()

# example of how to run this script from command line, with controls_per_collection=1, sample_size=18, date=14-10-2024, conditions_df='digflow/stock-conditions-locations.csv', conditions='digflow/inactivation-conditions.csv':
'''
python screen-initiate.py \
  -f 'test-folder' \
  -d 13-10-2025 \
  -s 18 \
  -c 1 \
  --conditions-df 'fly-stocks.csv' \
  --conditions 'inactivation-conditions.csv'
'''

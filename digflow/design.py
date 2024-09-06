import pandas as pd
import numpy as np
import os
import argparse
import random
import json
import tkinter as tk
from tkinter import messagebox, ttk
from datetime import datetime, timedelta

class Design:
    def __init__(self, wc_date, save_path=None, conditions=None, sample_size=None, experimenters=None, controls_per_collection=None, file=None):

        self.save_path = save_path
        if file==None: self.conditions = list(pd.read_csv(conditions, header=0).conditions)

        if sample_size!=None:
            if sample_size % 3 == 0:
                self.sample_size = int(sample_size / 3)  
            else: raise ValueError(f"Sample_size must be divisible by 3\nsample_size:{sample_size} is not divisible by 3")

        self.file = file
        self.amendment = None
        self.vials = pd.DataFrame(columns=['person', 'day', 'vials'])
        self.all_exps = None
        self.completed_exps = []
        self.remaining_exps = None
        self.shelves = []
        self.shelves_df = pd.DataFrame()
        self.shelf_template = pd.DataFrame([[ 1,24,  25,48,  49,72],
                                            [ 2,23,  26,47,  50,71],
                                            [ 3,22,  27,46,  51,70],
                                            [ 4,21,  28,45,  52,69],
                                            [ 5,20,  29,44,  53,68],
                                            [ 6,19,  30,43,  54,67],
                                            [ 7,18,  31,42,  55,66],
                                            [ 8,17,  32,41,  56,65],
                                            [ 9,16,  33,40,  57,64],
                                            [10,15,  34,39,  58,63],
                                            [11,14,  35,38,  59,62],
                                            [12,13,  36,37,  60,61]])
        self.shelf_total = 72 # for plugcamera set up
        self.controls_per_collection = controls_per_collection
        self.experimenters = experimenters
        self.date = wc_date

        self.check_if_monday(wc_date)
        self.wc_date = wc_date # Monday's date (to calculate when Tuesday and Wednesday collections happened)

        # work in progress
        if file!=None:
            with open(f'{file}/experiment.json', 'r') as f:
                json_data = json.load(f)
            self.conditions = json_data['conditions']
            self.experimenters = json_data['experimenters']
            self.remaining_exps = json_data['remaining']
            self.completed_exps = json_data['completed']
            self.controls_per_collection = json_data['controls_per_collection']

            # find amendments, e.g. failed experiments and add back to remaining_exps
            self.amendment = pd.read_csv(f'{file}/shelves.csv', index_col=0)

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

        if file==None:
            self.conditions_init(seed=42)
            self.first_experiment = True

    def conditions_init(self, seed):
        random.seed(seed)
        random.shuffle(self.conditions)
        self.all_exps = self.conditions * self.sample_size # add repeats after the conditions are shuffled, so they only repeat after each condition occurs once, twice, etc.
        self.remaining_exps = self.all_exps
        # note that self.all_exps and self.remaining_exps refer to sets of experiments. Here experiment has by default 3 replicates

    def check_if_monday(self, wc_date):
        monday_date = datetime.strptime(wc_date, "%d-%m-%Y")
        
        # Check if the date is a Monday (weekday() returns 0 for Monday)
        if monday_date.weekday() != 0:
            raise ValueError(f"The provided date, wc_date:{wc_date}, is not a Monday. Please use this format: DD-MM-YYYY")

    def calculate_dates(self, date_type):
        # Convert the Monday date string to a datetime object
        monday_date = datetime.strptime(self.wc_date, "%d-%m-%Y")

        if date_type=='collection':
            # Calculate Tuesday and Wednesday by adding 1 and 2 days to Monday
            tuesday_date = monday_date + timedelta(days=1)
            wednesday_date = monday_date + timedelta(days=2)
            
            # Return the dates as strings in the 'YYYY-MM-DD' format
            return tuesday_date.strftime("%d-%m-%Y"), wednesday_date.strftime("%d-%m-%Y")

        if date_type=='staging':
            # Calculate next Wed, Th, Fri by adding 9, 10, 11 days to Monday
            next_wed_date = monday_date + timedelta(days=9)
            next_th_date = monday_date + timedelta(days=10)
            next_fri_date = monday_date + timedelta(days=11)

            # Return the dates as strings in the 'YYYY-MM-DD' format
            return next_wed_date.strftime("%d-%m-%Y"), next_th_date.strftime("%d-%m-%Y"), next_fri_date.strftime("%d-%m-%Y")

    def add_vials(self, count, day, person):
        new_row = {'person': person,
                    'day': day,
                    'vials': count}

        new_row_df = pd.DataFrame([new_row])
        self.vials = pd.concat([self.vials, new_row_df], ignore_index=True)

    def build_shelves(self):
        experimenters = np.unique(self.vials.person)
        random.shuffle(experimenters)
        #print(experimenters)
        # Clear the shelves and shelves_df before building to avoid duplication
        self.shelves = []
        self.shelves_df = pd.DataFrame()

        for i, experimenter in enumerate(experimenters):
            shelf, shelf_df = self.build_shelf(experimenter=experimenter, shelf_num=i)
            #print(experimenter)
            if shelf is not None and not shelf_df.empty:
                # Only append non-empty shelves and data
                self.shelves.append(shelf)
                self.shelves_df = pd.concat([self.shelves_df, shelf_df], ignore_index=True)
    
    # note that this function uses hardcoded numbers for the shelf size
    def pc_to_rack(self, pc_num):
        # Check if the pc_num belongs to the bottom shelf (starts from 73)
        if pc_num >= 73:
            pc_num -= 72  # Normalize the pc_num to match the upper shelf pattern

        # Determine the rack based on pc_num
        if 1 <= pc_num <= 24:
            rack_num = 1
        elif 25 <= pc_num <= 48:
            rack_num = 2
        elif 49 <= pc_num <= 72:
            rack_num = 3
        else:
            raise ValueError("Invalid pc_num: out of range")
        return rack_num

    def build_shelf(self, experimenter, shelf_num):
        pattern = self.vials['person'] == experimenter
        vials_exp = self.vials[pattern]

        vials_day1 = vials_exp['day'] == 'Tuesday'
        vials_day2 = vials_exp['day'] == 'Wednesday'

        collection_day1 = vials_exp[vials_day1].vials.iloc[0]
        collection_day2 = vials_exp[vials_day2].vials.iloc[0]

        # Remove controls only if there are vials
        if collection_day1 > 0:
            collection_day1 -= self.controls_per_collection

        if collection_day2 > 0:
            collection_day2 -= self.controls_per_collection

        # Ensure collection numbers don't go negative
        collection_day1 = max(0, collection_day1)
        collection_day2 = max(0, collection_day2)

        # If both collection_day1 and collection_day2 are zero, skip this shelf
        if collection_day1 == 0 and collection_day2 == 0:
            return None, pd.DataFrame()

        # Initialize shelf shape, based on physical dimensions of the incubator
        default_value = '-'
        num_rows = 12
        num_columns = 6

        # Create the shelf structure
        shelf_structure = pd.DataFrame(np.full((num_rows, num_columns), default_value), columns=range(0, num_columns), index=range(0, num_rows))

        all_exps = self.remaining_exps

        # Select conditions for day 1 and day 2 without duplicates
        conditions_day1 = all_exps[:collection_day1]
        conditions_day2 = all_exps[collection_day1:collection_day1 + collection_day2]
        #print(conditions_day1)
        #print(conditions_day2)

        self.completed_exps = self.completed_exps + conditions_day1 + conditions_day2
        #print(self.completed_exps)
        self.remaining_exps = all_exps[len(conditions_day1 + conditions_day2):]

        empty = 24 - len(conditions_day1) - len(conditions_day2) - self.controls_per_collection * 2

        conditions = conditions_day1 + ['control'] * self.controls_per_collection + conditions_day2 + ['control'] * self.controls_per_collection + ['-'] * empty
        date_day1 = self.calculate_dates(date_type='collection')[0]
        date_day2 = self.calculate_dates(date_type='collection')[1]
        collection_meta = [f'{date_day1}'] * len(conditions_day1 + ['control'] * self.controls_per_collection) + [f'{date_day2}'] * len(conditions_day2 + ['control'] * self.controls_per_collection) + [''] * empty

        conditions_meta = list(zip(conditions, collection_meta))

        random.shuffle(conditions_meta)

            # Distribute the conditions across the shelf
        def fill_columns_with_conditions(shelf, conditions, col_indices, shelf_num, date):
            # Assign the shuffled conditions to the specified columns
            positions = []
            for idx, (row, col) in enumerate([(row, col) for col in col_indices for row in range(len(shelf.index))]):
                shelf.iloc[row, col] = conditions[idx][0]

                # populate dataframe with non-empty conditions
                if conditions[idx][0] != '-':
                    pc_num = self.shelf_template.iloc[row,col]+(self.shelf_total*shelf_num)
                    index = {'experimenter': experimenter,
                            'collector': experimenter,
                            'shelf': shelf_num + 1,
                            'rack': self.pc_to_rack(pc_num),
                            'plugcamera': f'pc{pc_num}',
                            'condition': conditions[idx][0],
                            'collection_date': conditions[idx][1],
                            'staging_date': date,
                            'amendments': '',
                            'plugcamera_pos': self.shelf_template.iloc[row,col]+(self.shelf_total*shelf_num)}

                    positions.append(index)

            df = pd.DataFrame(positions)
            return shelf, df

        # Staging dates (you already calculate them earlier in the function)
        staging_day1, staging_day2, staging_day3 = self.calculate_dates(date_type='staging')

        # Now, fill the columns with conditions
        shelf_structure, df_1 = fill_columns_with_conditions(shelf_structure, conditions_meta, [0, 1], shelf_num, staging_day1)
        random.shuffle(conditions_meta)
        shelf_structure, df_2 = fill_columns_with_conditions(shelf_structure, conditions_meta, [2, 3], shelf_num, staging_day2)
        random.shuffle(conditions_meta)
        shelf_structure, df_3 = fill_columns_with_conditions(shelf_structure, conditions_meta, [4, 5], shelf_num, staging_day3)

        # Combine all shelf data
        shelf_df = pd.concat([df_1, df_2, df_3])
        shelf_df = shelf_df.sort_values('plugcamera_pos', ascending=True)
        shelf_df = shelf_df.drop(columns=['plugcamera_pos'])

        return shelf_structure, shelf_df

    def output(self):
        save_path = f'{self.save_path}/{self.date}'
        os.makedirs(save_path, exist_ok=True)
        
        # Save the shelves dataframe if it has data
        if not self.shelves_df.empty:
            self.shelves_df.to_csv(f'{save_path}/shelves.csv')

        # Save individual shelf layouts only if the shelves exist
        for idx, shelf in enumerate(self.shelves):
            if shelf is not None:
                shelf.to_csv(f'{save_path}/shelf{idx + 1}_layout.csv')

        # Save the experiment JSON
        experiment_dict = {'conditions': self.conditions,
                            'experimenters': self.experimenters,
                            'remaining': self.remaining_exps,
                            'completed': self.completed_exps,
                            'controls_per_collection': self.controls_per_collection}

        with open(f'{save_path}/experiment.json', 'w') as f:
            json.dump(experiment_dict, f, indent=4)

    def vials_gui(self):
        # Function to validate data before final submission
        def validate_vials():
            errors = []  # Collect all validation errors here

            # Group the vials data by person and day to count the number of entries for each person
            vials_per_person_day = self.vials.groupby(['person', 'day']).size().unstack(fill_value=0)

            invalid_entries = []  # To store offending entries to be removed later

            for person in vials_per_person_day.index:
                # Skip validation if the person has no entries (i.e., both Tuesday and Wednesday are 0)
                if vials_per_person_day.loc[person, 'Tuesday'] == 0 and vials_per_person_day.loc[person, 'Wednesday'] == 0:
                    continue  # No need to validate if no entries exist

                # Check if there's more than one entry for Tuesday
                if vials_per_person_day.loc[person, 'Tuesday'] > 1:
                    # Collect all invalid Tuesday entries
                    invalid_entries.append(self.vials[(self.vials['person'] == person) & (self.vials['day'] == 'Tuesday')].index.tolist())
                    errors.append(f"{person} has multiple entries for Tuesday. The invalid entries will be removed.")

                # Check if there's more than one entry for Wednesday
                if vials_per_person_day.loc[person, 'Wednesday'] > 1:
                    # Collect all invalid Wednesday entries
                    invalid_entries.append(self.vials[(self.vials['person'] == person) & (self.vials['day'] == 'Wednesday')].index.tolist())
                    errors.append(f"{person} has multiple entries for Wednesday. The invalid entries will be removed.")

                # Check if there's no entry for Tuesday
                if vials_per_person_day.loc[person, 'Tuesday'] == 0:
                    errors.append(f"{person} has no entry for Tuesday. Please add an entry for Tuesday.")

                # Check if there's no entry for Wednesday
                if vials_per_person_day.loc[person, 'Wednesday'] == 0:
                    errors.append(f"{person} has no entry for Wednesday. Please add an entry for Wednesday.")

            # Remove all invalid entries (flatten the list of indexes)
            invalid_indexes = [item for sublist in invalid_entries for item in sublist]
            if invalid_indexes:
                self.vials.drop(invalid_indexes, inplace=True)

            # Return all collected errors, or None if no errors were found
            return "\n".join(errors) if errors else None

        # Function to handle final submission
        def final_submit():
            # Validate the vials data before submitting
            validation_error = validate_vials()
            if validation_error:
                messagebox.showerror("Validation Error", validation_error)
                update_display()  # Update the display with the valid data after removing the invalid entries
                return

            # If validation passes, proceed with the next steps
            self.build_shelves()  # Proceed with building shelves
            self.output()  # Output the results after building shelves

            # Close the GUI window after submission to prevent further changes
            root.destroy()

        # Function to enter data (when adding individual vials)
        def submit_entry():
            person = person_var.get()
            day = day_var.get()
            count_str = vials_var.get()

            if not person or not day or not count_str:
                messagebox.showerror("Input Error", "All fields are required")
                return

            try:
                count = int(count_str)
            except ValueError:
                messagebox.showerror("Input Error", "Vials count must be an integer")
                return

            # Add the entered data to the vials DataFrame
            self.add_vials(count, day, person)

            # Update the display after data entry
            update_display()

        def clear_display():
            text_display.delete(1.0, tk.END)

        def update_display():
            clear_display()
            display_text = self.vials.to_string(index=False)
            text_display.insert(tk.END, display_text)

        # Initialize the GUI window
        root = tk.Tk()
        root.title("Vials Data Entry")

        tk.Label(root, text="Person").grid(row=0, column=0, padx=10, pady=5)
        tk.Label(root, text="Day").grid(row=1, column=0, padx=10, pady=5)
        tk.Label(root, text="Vials").grid(row=2, column=0, padx=10, pady=5)

        person_var = tk.StringVar()
        day_var = tk.StringVar()
        vials_var = tk.StringVar()

        # Use self.experimenters for person options
        person_options = self.experimenters
        day_options = ["Tuesday", "Wednesday"]
        vials_options = [str(i) for i in range(25)]

        person_menu = ttk.Combobox(root, textvariable=person_var, values=person_options, state="readonly")
        day_menu = ttk.Combobox(root, textvariable=day_var, values=day_options, state="readonly")
        vials_menu = ttk.Combobox(root, textvariable=vials_var, values=vials_options, state="readonly")

        person_menu.grid(row=0, column=1, padx=5, pady=5)
        day_menu.grid(row=1, column=1, padx=5, pady=5)
        vials_menu.grid(row=2, column=1, padx=5, pady=5)

        tk.Button(root, text='Add Entry', command=submit_entry).grid(row=3, column=1, sticky=tk.W, pady=4)
        tk.Button(root, text='Submit Data', command=final_submit).grid(row=3, column=3, sticky=tk.W, pady=4)

        text_display = tk.Text(root, height=10, width=50)
        text_display.grid(row=4, column=0, columnspan=3, padx=10, pady=10)

        root.mainloop()

'''
wc_date = '09-09-2024'
save_path = 'output'
sample_size = 15
conditions = 'conditions.csv'
experimenters = ['Lucy', 'Lena', 'Alice', 'Anna', 'Michael']
control_sample_size = 1

design = Design(wc_date=wc_date, save_path=save_path,sample_size=sample_size, conditions=conditions, experimenters=experimenters, controls_per_collection=control_sample_size)
design.vials_gui()
design.output()
'''
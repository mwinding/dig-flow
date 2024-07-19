# %%
import pandas as pd
import numpy as np
import os
import argparse
import random
import json
import tkinter as tk
from tkinter import messagebox, ttk

class Design:
    def __init__(self, save_path=None, conditions=None, sample_size=None, experimenters=None, file=None, amendment=None):

        self.save_path = save_path
        self.conditions = conditions
        self.sample_size = sample_size
        self.file = file
        self.amendment = amendment
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
        self.experimenters = experimenters

        # work in progress
        if file!=None:
            with open(file, 'r') as f:
                json_data = json.load(f)
            self.conditions = json_data['conditions']
            self.experimenters = json_data['experimenters']
            self.remaining_exps = json_data['remaining']
            self.completed_exps = json_data['completed']

        # find amendments, e.g. failed experiments and add back to remaining_exps
        if amendment!=None:
            self.amendment = pd.read_csv(amendment, index_col=0)
            amend_bool = self.amendment.amendments == -1
            to_remove = self.amendment[amend_bool].condition.values

            # Create a copy of the original list to avoid modifying it while iterating
            filtered_list = self.completed_exps.copy()

            # Iterate over the to_remove list and remove elements from the right side
            for item in to_remove:
                # Find the index of the item starting from the end and remove it
                index = len(filtered_list) - 1 - filtered_list[::-1].index(item)
                filtered_list.pop(index)

            self.completed_exps = filtered_list

        if file==None:
            self.conditions_init(seed=42)
            self.first_experiment = True

    def conditions_init(self, seed):
        random.seed(seed)
        random.shuffle(self.conditions)
        self.all_exps = self.conditions * self.sample_size # add repeats after the conditions are shuffled, so they only repeat after each condition occurs once, twice, etc.
        self.remaining_exps = self.all_exps

    def add_vials(self, count, day, person):
        new_row = {'person': person,
                    'day': day,
                    'vials': count}

        new_row_df = pd.DataFrame([new_row])
        self.vials = pd.concat([self.vials, new_row_df], ignore_index=True)

    def build_shelves(self):
        experimenters = np.unique(self.vials.person)
        random.shuffle(experimenters)
        num_shelves = len(experimenters)

        for i, experimenter in enumerate(experimenters):
             shelf, shelf_df = self.build_shelf(experimenter=experimenter, shelf_num=i)
             self.shelves.append(shelf) # need to add meta-data to this...
             self.shelves_df = pd.concat([self.shelves_df,shelf_df], ignore_index=True)

    def build_shelf(self, experimenter, shelf_num):

        pattern = self.vials['person'] == experimenter
        vials_exp = self.vials[pattern]

        vials_day1 = vials_exp['day'] == 'Tuesday'
        vials_day2 = vials_exp['day'] == 'Wednesday'

        collection_day1 = vials_exp[vials_day1].vials.iloc[0]
        collection_day2 = vials_exp[vials_day2].vials.iloc[0]

        # remove one for control position
        collection_day1 = collection_day1 - 1
        collection_day2 = collection_day2 - 1

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

        self.completed_exps = self.completed_exps + conditions_day1 + conditions_day2
        self.remaining_exps = all_exps[len(conditions_day1+conditions_day2):]

        empty_day1 = 24 - len(conditions_day1)
        empty_day2 = 24 - len(conditions_day2)

        conditions_day1 = conditions_day1 + ['-']*empty_day1 + ['control']
        conditions_day2 = conditions_day2 + ['-']*empty_day2 + ['control']

        random.shuffle(conditions_day1)
        random.shuffle(conditions_day2)

        # Distribute the conditions across the shelf
        def fill_columns_with_conditions(shelf, conditions, col_indices, shelf_num):
            # Assign the shuffled conditions to the specified columns
            positions = []
            for idx, (row, col) in enumerate([(row, col) for col in col_indices for row in range(len(shelf.index))]):
                shelf.iloc[row, col] = conditions[idx]

                # populate dataframe with non-empty conditions
                if conditions[idx] != '-':
                    index = {'experimenter': experimenter, 
                            'shelf': shelf_num + 1, 
                            'plugcamera': f'pc{self.shelf_template.iloc[row,col]+(self.shelf_total*shelf_num)}', 
                            'condition': conditions[idx],
                            'amendments': '',
                            'plugcamera_pos': self.shelf_template.iloc[row,col]+(self.shelf_total*shelf_num)}

                    positions.append(index)

            df = pd.DataFrame(positions)    
            return shelf, df

        # Fill columns with conditions
        shelf_structure, df_1 = fill_columns_with_conditions(shelf_structure, conditions_day1, [0, 1], shelf_num)
        random.shuffle(conditions_day1)
        shelf_structure, df_2 = fill_columns_with_conditions(shelf_structure, conditions_day1, [2, 3], shelf_num)
        shelf_structure, df_3 = fill_columns_with_conditions(shelf_structure, conditions_day2, [4, 5], shelf_num)

        shelf_df = pd.concat([df_1, df_2, df_3])
        shelf_df = shelf_df.sort_values('plugcamera_pos', ascending=True)
        shelf_df = shelf_df.drop(columns=['plugcamera_pos'])

        return shelf_structure, shelf_df

    def output(self):
        self.shelves_df.to_csv(f'{self.save_path}/shelves.csv')
        self.shelves[0].to_csv(f'{self.save_path}/shelf1_layout.csv')
        self.shelves[1].to_csv(f'{self.save_path}/shelf2_layout.csv')

        experiment_dict = {'conditions': self.conditions,
                            'experimenters': self.experimenters,
                            'remaining': self.remaining_exps,
                            'completed': self.completed_exps}

        with open(f'{self.save_path}/experiment.json', 'w') as f:
            json.dump(experiment_dict, f, indent=4)

    def vials_gui(self):
        # function to enter data
        def submit():
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
            
            self.add_vials(count, day, person)
            update_display()

        # Function to handle ending the application
        def end_program():
            if messagebox.askokcancel("Quit", "Do you want to quit and save data?"):
                root.destroy()

        def clear_display():
            text_display.delete(1.0, tk.END)
        
        def update_display():
            clear_display()
            display_text = self.vials.to_string(index=False)
            text_display.insert(tk.END, display_text)

        root = tk.Tk()
        root.title("Vials Data Entry")

        tk.Label(root, text="Person").grid(row=0, column=0, padx=10, pady=5)
        tk.Label(root, text="Day").grid(row=1, column=0, padx=10, pady=5)
        tk.Label(root, text="Vials").grid(row=2, column=0, padx=10, pady=5)

        person_var = tk.StringVar()
        day_var = tk.StringVar()
        vials_var = tk.StringVar()

        person_options = ["Lucy", "Lena"]
        day_options = ["Tuesday", "Wednesday"]
        vials_options = [str(i) for i in range(25)]

        person_menu = ttk.Combobox(root, textvariable=person_var, values=person_options, state="readonly")
        day_menu = ttk.Combobox(root, textvariable=day_var, values=day_options, state="readonly")
        vials_menu = ttk.Combobox(root, textvariable=vials_var, values=vials_options, state="readonly")

        person_menu.grid(row=0, column=1, padx=5, pady=5)
        day_menu.grid(row=1, column=1, padx=5, pady=5)
        vials_menu.grid(row=2, column=1, padx=5, pady=5)

        tk.Button(root, text='Submit', command=submit).grid(row=3, column=1, sticky=tk.W, pady=4)
        tk.Button(root, text='End', command=end_program).grid(row=3, column=2, sticky=tk.W, pady=4)

        text_display = tk.Text(root, height=10, width=50)
        text_display.grid(row=4, column=0, columnspan=2, padx=10, pady=10)

        root.mainloop()

# Example usage
design = Design(save_path='./output/',sample_size=10, conditions=[f'GAL4-{x}' for x in range(1, 31)], experimenters=['Lucy','Lena'])
design.vials_gui()
design.build_shelves()
design.output()

# %%
design2 = Design(save_path='output/',file='output/experiment.json', amendment='output/shelves.csv')


# %%

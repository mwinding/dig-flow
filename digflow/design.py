# %%
import pandas as pd
import numpy as np
import os
import argparse
import random
import tkinter as tk
from tkinter import messagebox, ttk

class Design:
    def __init__(self, conditions=None, sample_size=None, file=None, amendment=None):

        self.conditions = conditions
        self.sample_size = sample_size
        self.file = file
        self.amendment = amendment
        self.vials = pd.DataFrame(columns=['person', 'day', 'vials'])
        self.all_exps = None
        self.completed_exps = []
        self.remaining_exps = None
        self.shelves = None
        self.shelves_df = None
        #self.experimenters = experimenters

        # work in progress
        if file!=None:
            loaded_data = pd.read_csv(file)
            self.conditions = loaded_data.conditions
            self.experimenters = loaded_data.experimenters.values[0]
            self.remaining_exps = loaded_data.remaining_exps
            self.completed_exps = loaded_data.completed_exps

        if file==None:
            self.conditions_init(seed=42)

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
        num_shelves = len(experimenters)
        for experimenter in experimenters:
            self.shelves, self.shelves_df = self.build_shelf(experimenter=experimenter)

    def build_shelf(self, experimenter):

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
        def fill_columns_with_conditions(shelf, conditions, col_indices):
            # Assign the shuffled conditions to the specified columns
            for idx, (row, col) in enumerate([(row, col) for col in col_indices for row in range(len(shelf.index))]):
                shelf.iloc[row, col] = conditions[idx]

            return shelf

        # Fill columns with conditions
        shelf_structure = fill_columns_with_conditions(shelf_structure, conditions_day1, [0, 1])
        random.shuffle(conditions_day1)
        shelf_structure = fill_columns_with_conditions(shelf_structure, conditions_day1, [2, 3])
        shelf_structure = fill_columns_with_conditions(shelf_structure, conditions_day2, [4, 5])


        # Create a dataframe
        shelf_df = pd.DataFrame(shelf_structure)

        # Flatten the dataframe into a list with positions from 1-72
        flattened = []
        for col in shelf_df.columns:
            for value in shelf_df[col]:
                flattened.append(value)

        # Create a new dataframe with positions
        start_position = 1
        positions = range(start_position, len(flattened) + 1)
        shelf_df = pd.DataFrame({'plugcamera': [f'pc{x}' for x in positions], 'Value': flattened})

        return shelf_structure, shelf_df

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
design = Design(sample_size=10, conditions=[f'GAL4-{x}' for x in range(1, 31)])
design.vials_gui()
design.build_shelves()
# %%

import pandas as pd
import numpy as np
import os
import argparse
import random
import json
from datetime import datetime, timedelta

# ---------- helpers ----------
def check_monday(wc_date_str: str):
    d = datetime.strptime(wc_date_str, "%d-%m-%Y")
    if d.weekday() != 0:
        raise ValueError(f"The provided date, wc_date:{wc_date_str}, is not a Monday. Use DD-MM-YYYY.")

def calculate_dates(wc_date_str: str):
    """Return the 6 staging 'dates' as strings:
       Tue–Wed (range), Wed, Wed–Thu (range), Thu, Thu–Fri (range), Fri."""
    monday = datetime.strptime(wc_date_str, "%d-%m-%Y")

    def fmt_range(a: datetime, b: datetime) -> str:
        if b < a:
            a, b = b, a
        if a.month == b.month and a.year == b.year:
            return f"{a.day}-{b.day:02d}/{a.month:02d}/{a.year}"
        return f"{a.strftime('%d/%m/%Y')}-{b.strftime('%d/%m/%Y')}"

    tues  = monday + timedelta(days=1)
    wed   = monday + timedelta(days=2)
    thurs = monday + timedelta(days=3)
    fri   = monday + timedelta(days=4)

    tues_night  = fmt_range(tues, wed)
    wed_night   = fmt_range(wed, thurs)
    thurs_night = fmt_range(thurs, fri)

    return (tues_night, wed.strftime("%d/%m/%Y"),
            wed_night,  thurs.strftime("%d/%m/%Y"),
            thurs_night, fri.strftime("%d/%m/%Y"))

def load_conditions(conditions_csv: str) -> list[str]:
    df = pd.read_csv(conditions_csv, header=0)
    if 'conditions' not in df.columns:
        raise ValueError(f"'conditions' column not found in {conditions_csv}")
    conds = (
        df['conditions']
        .dropna()
        .astype(str)
        .str.strip()
    )
    conds = conds[conds.ne('')].tolist()
    if not conds:
        raise ValueError(f"No usable conditions in {conditions_csv}")
    return conds

def load_stock_df(conditions_df_csv: str) -> pd.DataFrame:
    if conditions_df_csv is None:
        raise ValueError("Provide --conditions-df (stock locations CSV) for a new experiment.")
    sdf = pd.read_csv(conditions_df_csv, header=0).copy()
    # Normalise keys and build location
    sdf['condition'] = sdf['ID'].astype(str).str.strip()
    sdf['location']  = sdf['Tray'].astype(str).str.strip() + '-' + sdf['Location'].astype(str).str.strip()
    return sdf[['condition', 'location']]

def link_conditions_with_locations(conditions: list[str], stock_df: pd.DataFrame) -> dict[str, str]:
    exp_df = pd.DataFrame({'condition': pd.Series(conditions, dtype=str).str.strip()})
    merged = exp_df.merge(stock_df, on='condition', how='left')
    return dict(zip(merged['condition'], merged['location']))

def build_shelves_df(dates, inc_conds, controls_per_collection, condition_locations):
    cols = ['experimenter','collector','incubator','shelf','rack','plugcamera',
            'condition','location','staging_date','amendments','comments','staging_times']
    df = pd.DataFrame(columns=cols)

    for idx, date in enumerate(dates):
        is_range = '-' in date  # e.g. "15-16/10/2024"
        shelf_num = 2 if is_range else 1
        rack_num  = (idx // 2) + 1

        # For this date: incubator 1 first, then incubator 2
        for incubator in (1, 2):
            shelf_conditions = inc_conds[incubator].copy()
            for _ in range(controls_per_collection):
                shelf_conditions.append('control')
            random.shuffle(shelf_conditions)

            rows = []
            for condition in shelf_conditions:
                location = condition_locations.get(condition, '')
                rows.append({
                    'experimenter': '',
                    'collector': '',
                    'incubator': incubator,
                    'shelf': shelf_num,
                    'rack': rack_num,
                    'plugcamera': '',
                    'condition': condition,
                    'location': location,
                    'staging_date': date,
                    'amendments': '',
                    'comments': '',
                    'staging_times': ''
                })
            df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)

    return df

# ---------- main ----------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='set up initial spreadsheet for inactivation screen (week 1)')
    parser.add_argument('-f', '--file-path', dest='file_path', type=str, required=True,
                        help='path to save folder for inactivation conditions')
    parser.add_argument('-d', '--date', dest='wc_date', type=str, required=True,
                        help='date of the Monday when the week starts, format: DD-MM-YYYY')
    parser.add_argument('-s', '--sample-size', dest='sample_size', type=int, required=True,
                        help='number of times each condition is repeated (must be divisible by 6)')
    parser.add_argument('-c', '--controls-per-collection', dest='controls_per_collection', type=int, required=True,
                        help='number of control experiments to include per collection day')
    parser.add_argument('--conditions-df', dest='conditions_df', type=str, required=True,
                        help='path to conditions locations CSV (stock table with ID/Tray/Location)')
    parser.add_argument('--conditions', dest='conditions', type=str, required=True,
                        help='path to conditions CSV with a "conditions" column')

    args = parser.parse_args()

    # Validate date and sample size
    check_monday(args.wc_date)
    if args.sample_size % 6 != 0:
        raise ValueError(f"sample_size must be divisible by 6; got {args.sample_size}")
    repeats_factor = args.sample_size // 6

    # Prepare output path
    save_path = os.path.join(args.file_path, args.wc_date)
    os.makedirs(save_path, exist_ok=True)

    # Load inputs
    random.seed(42)
    conditions_list = load_conditions(args.conditions)
    stock_df = load_stock_df(args.conditions_df)
    condition_locations = link_conditions_with_locations(conditions_list, stock_df)

    # Build weekly plan
    dates = calculate_dates(args.wc_date)
    needed_per_inc = 24 - args.controls_per_collection  # e.g. 23 when c=1

    # Shuffle and expand once; allocate 23 to each incubator
    pool = conditions_list.copy()
    random.shuffle(pool)
    expanded = pool * repeats_factor

    if len(expanded) < needed_per_inc * 2:
        raise ValueError(
            f"Not enough conditions for two incubators this week. Need {needed_per_inc*2}, "
            f"have {len(expanded)} (unique={len(conditions_list)}, repeats_factor={repeats_factor})."
        )

    inc_conds = {
        1: expanded[:needed_per_inc],
        2: expanded[needed_per_inc:needed_per_inc*2],
    }
    remaining_exps = expanded[needed_per_inc*2:]
    completed_exps = inc_conds[1] + inc_conds[2]

    shelves_df = build_shelves_df(
        dates=dates,
        inc_conds=inc_conds,
        controls_per_collection=args.controls_per_collection,
        condition_locations=condition_locations
    )
    shelves_df.to_csv(os.path.join(save_path, 'shelves.csv'), index=False)

    # Write experiment.json for future weeks
    experiment_dict = {
        'conditions': conditions_list,
        'remaining': remaining_exps,
        'completed': completed_exps,
        'controls_per_collection': args.controls_per_collection,
        'condition_locations': condition_locations
    }
    with open(os.path.join(save_path, 'experiment.json'), 'w') as f:
        json.dump(experiment_dict, f, indent=4)

    print(f"Wrote {os.path.join(save_path, 'shelves.csv')}")
    print(f"Wrote {os.path.join(save_path, 'experiment.json')}")

#!/usr/bin/env python3
import argparse
import os
import re
import json
from datetime import datetime, timedelta
import pandas as pd
import random

# Folders are now YYYY-MM-DD
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")  # YYYY-MM-DD

# ----------------- date helpers -----------------
def parse_monday(date_str_folder: str) -> datetime:
    """Parse a Monday in folder format YYYY-MM-DD."""
    d = datetime.strptime(date_str_folder, "%Y-%m-%d")
    if d.weekday() != 0:
        raise ValueError(f"{date_str_folder} is not a Monday (YYYY-MM-DD).")
    return d

def format_ddmmyyyy(d: datetime) -> str:
    """Return DD-MM-YYYY for human-facing/staging helper inputs."""
    return d.strftime("%d-%m-%Y")

def format_yyyymmdd(d: datetime) -> str:
    """Return YYYY-MM-DD for folder names."""
    return d.strftime("%Y-%m-%d")

def calculate_dates(wc_date_str_ddmmyyyy: str):
    """Return the 6 staging 'dates' as strings:
       Tue–Wed (range), Wed, Wed–Thu (range), Thu, Thu–Fri (range), Fri.
       Input: DD-MM-YYYY (Monday)."""
    monday = datetime.strptime(wc_date_str_ddmmyyyy, "%d-%m-%Y")

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

# ----------------- experiment/master helpers -----------------
def load_experiment(exp_path: str) -> dict:
    with open(exp_path, "r") as f:
        data = json.load(f)
    required = [
        "conditions",
        "remaining",
        "completed_counts",
        "replicates_per_experiment",
        "controls_per_collection",
        "condition_locations",
    ]
    for k in required:
        if k not in data:
            raise ValueError(f"Missing '{k}' in {exp_path}")
    data["controls_per_collection"] = int(data["controls_per_collection"])
    data["replicates_per_experiment"] = int(data["replicates_per_experiment"])
    if not isinstance(data["completed_counts"], dict):
        raise ValueError("'completed_counts' must be a dict of condition -> int")
    # Optional fields (new name)
    if "failure_counts" not in data or not isinstance(data["failure_counts"], dict):
        data["failure_counts"] = {}
    # NEW: overall target per condition, if present
    if "target_replicates_total" in data:
        try:
            data["target_replicates_total"] = int(data["target_replicates_total"])
        except Exception:
            pass
    return data

def list_date_subfolders(root: str):
    subs = []
    for entry in os.listdir(root):
        full = os.path.join(root, entry)
        if os.path.isdir(full) and DATE_RE.match(entry):
            subs.append(entry)
    subs.sort(key=lambda s: datetime.strptime(s, "%Y-%m-%d"))
    return subs

def rebuild_master_df(root: str, date_folders):
    frames = []
    for d in date_folders:
        shelves_path = os.path.join(root, d, "shelves.csv")
        if not os.path.exists(shelves_path):
            continue
        df = pd.read_csv(shelves_path)
        if "condition" not in df.columns or "amendments" not in df.columns:
            raise ValueError(f"{shelves_path} must contain 'condition' and 'amendments' columns.")
        df = df.copy()
        df["condition"] = df["condition"].astype(str).str.strip()
        am = df["amendments"]
        # normalise booleans for -1 flags
        df["_is_neg1"] = (am == -1) | (am.astype(str).str.strip() == "-1")
        df["_is_success"] = (~df["_is_neg1"]) & (df["condition"].str.lower() != "control")
        df["_week"] = d  # now YYYY-MM-DD
        frames.append(df)
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame(columns=["condition", "amendments", "_is_neg1", "_is_success", "_week"])

def write_master_files(root: str, current_week_dir: str, master_df: pd.DataFrame, root_name: str):
    """Write master to ROOT and a timestamped snapshot in CURRENT week."""
    os.makedirs(root, exist_ok=True)
    root_master_path = os.path.join(root, root_name)
    master_df.to_csv(root_master_path, index=False)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    snapshot_name = f"master-file_{ts}.csv"
    snapshot_path = os.path.join(current_week_dir, snapshot_name)
    master_df.to_csv(snapshot_path, index=False)

    return root_master_path, snapshot_path

def cumulative_success_counts(master_df: pd.DataFrame) -> dict:
    """Return total successful replicates per condition across ALL weeks."""
    if master_df.empty:
        return {}
    ok = master_df[master_df["_is_success"]]
    if ok.empty:
        return {}
    return ok.groupby("condition").size().to_dict()

def cumulative_failure_counts(master_df: pd.DataFrame) -> dict:
    """Return total (-1) counts per condition across ALL weeks (excluding control rows)."""
    if master_df.empty:
        return {}
    fail = master_df[(master_df["_is_neg1"]) & (master_df["condition"].str.lower() != "control")]
    if fail.empty:
        return {}
    return fail.groupby("condition").size().to_dict()

def select_next_week(remaining: list, per_inc: int):
    """Take next per_inc for incubator 1, then per_inc for incubator 2 (soft-fill if short)."""
    r = list(remaining)
    inc1 = r[:min(per_inc, len(r))]
    r = r[len(inc1):]
    inc2 = r[:min(per_inc, len(r))]
    r = r[len(inc2):]
    return inc1, inc2, r

def write_timestamped_experiment(dir_path: str, payload: dict) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(dir_path, f"experiment_{ts}.json")
    with open(out_path, "w") as f:
        json.dump(payload, f, indent=4)
    return out_path

# ----------------- shelves-building -----------------
def make_fixed_layout(base_conditions, controls_per_collection):
    """Create a single, fixed layout (conditions + controls) for an incubator (≤ 24 rows total)."""
    layout = base_conditions.copy()
    layout.extend(['control'] * controls_per_collection)
    random.shuffle(layout)  # different each run
    return layout

def build_shelves_df(dates, inc_layout, condition_locations):
    """
    Build shelves with:
      - fixed per-incubator condition+control layout reused for every date
      - per-rack random permutation per incubator to avoid shelf collisions within a rack
      - if there are fewer than 24 items, we just emit fewer rows (no blank rows)
    """
    cols = ['experimenter','collector','incubator','shelf','rack','plugcamera',
            'condition','location','staging_date','amendments','comments','staging_times']
    df = pd.DataFrame(columns=cols)

    num_racks = (len(dates) + 1) // 2  # 6 dates -> 3 racks

    # random shelf order per rack per incubator
    rack_shelf_map = {
        inc: {r: random.sample([1, 2], 2) for r in range(num_racks)}
        for inc in (1, 2)
    }

    for idx, date in enumerate(dates):
        rack_idx = idx // 2          # 0,0,1,1,2,2
        pos_in_pair = idx % 2        # 0 first date in the rack, 1 second
        rack_num  = rack_idx + 1

        for incubator in (1, 2):
            shelf_num = rack_shelf_map[incubator][rack_idx][pos_in_pair]
            shelf_conditions = inc_layout.get(incubator, [])
            if not shelf_conditions:
                continue

            rows = []
            for condition in shelf_conditions:
                location = condition_locations.get(condition, '')
                rows.append({
                    'experimenter': '',
                    'collector': '',
                    'incubator': f'incubator-{incubator}',
                    'shelf': f'shelf-{shelf_num}',
                    'rack': f'rack-{rack_num}',
                    'plugcamera': '',
                    'condition': condition,
                    'location': location,
                    'staging_date': date,
                    'amendments': '',
                    'comments': '',
                    'staging_times': ''
                })
            if rows:
                df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)

    return df

# ----------------- completion check -----------------
def all_conditions_complete(completed_counts: dict, target_reps: int, conditions: list[str]) -> bool:
    """True if every condition has completed_counts >= target_reps."""
    for c in conditions:
        if completed_counts.get(c, 0) < target_reps:
            return False
    return True

# ----------------- main -----------------
def main():
    parser = argparse.ArgumentParser(
        description="Weekly roll-forward: rebuild master, incorporate late -1 without double-counting, recompute successes (completed_counts), pick next batch, and write next week."
    )
    parser.add_argument("-f", "--folder", required=True, help="Root folder containing dated subfolders (e.g., test-folder)")
    parser.add_argument("-d", "--date", required=True, help="Most recent date folder name (YYYY-MM-DD), e.g., 2025-10-13")
    parser.add_argument("--next-date", default=None, help="Override next week folder (YYYY-MM-DD). Defaults to current Monday + 7 days.")
    parser.add_argument("--master-name", default="master-file.csv", help="Filename for the master CSV at root (default: master-file.csv)")
    parser.add_argument("--emit-next-picks", action="store_true", help="Also write next_conditions.json in the NEXT week folder")
    parser.add_argument("-p", "--per-incubator-conditions", dest="per_incubator_conditions",
                        type=int, default=None,
                        help="Number of CONDITIONS per incubator for NEXT week (controls added on top). "
                             "Default: 24 - controls_per_collection from experiment.json")
    args = parser.parse_args()

    # Paths & dates
    root = os.path.abspath(args.folder)
    current_week_dir = os.path.join(root, args.date)
    if not os.path.isdir(current_week_dir):
        raise FileNotFoundError(f"Date folder not found: {current_week_dir}")

    current_monday = parse_monday(args.date)
    if args.next_date:
        next_monday = parse_monday(args.next_date)
    else:
        next_monday = current_monday + timedelta(days=7)

    # Folder name (YYYY-MM-DD) and display string (DD-MM-YYYY)
    next_date_folder = format_yyyymmdd(next_monday)
    next_date_display = format_ddmmyyyy(next_monday)

    next_week_dir = os.path.join(root, next_date_folder)
    os.makedirs(next_week_dir, exist_ok=True)

    exp_path = os.path.join(current_week_dir, "experiment.json")
    shelves_path = os.path.join(current_week_dir, "shelves.csv")
    if not os.path.exists(exp_path):
        raise FileNotFoundError(f"experiment.json not found in {current_week_dir}")
    if not os.path.exists(shelves_path):
        raise FileNotFoundError(f"shelves.csv not found in {current_week_dir}")

    # 1) Load experiment.json from CURRENT week
    exp = load_experiment(exp_path)

    controls_per_collection = exp["controls_per_collection"]
    default_per_inc = 24 - controls_per_collection
    per_inc = args.per_incubator_conditions if args.per_incubator_conditions is not None else default_per_inc

    # 2) Rebuild master (root) + snapshot (current week)
    date_folders = list_date_subfolders(root)  # now matches YYYY-MM-DD folders
    master_df = rebuild_master_df(root, date_folders)
    root_master_path, snapshot_path = write_master_files(root, current_week_dir, master_df, args.master_name)

    # 3) Recompute successes (literal N) across ALL weeks
    success_totals = cumulative_success_counts(master_df)
    new_completed_counts = {c: int(success_totals.get(c, 0)) for c in exp["conditions"]}

    # 4) Late-entered failures: compute cumulative failures and append ONLY the delta since last run
    prev_failure_counts = exp.get("failure_counts", {}) or {}
    prev_failure_counts = {k: int(v) for k, v in prev_failure_counts.items()}
    failure_totals = cumulative_failure_counts(master_df)  # cumulative across all time

    newly_appended = {}
    remaining = list(exp["remaining"])
    for cond, total_fails in failure_totals.items():
        prev = prev_failure_counts.get(cond, 0)
        delta = int(total_fails) - int(prev)
        if delta > 0:
            remaining.extend([cond] * delta)
            newly_appended[cond] = delta
    new_failure_counts = {**prev_failure_counts, **{k: int(v) for k, v in failure_totals.items()}}

    # 5) Select next batch (soft-fill if short)
    inc1, inc2, new_remaining = select_next_week(remaining, per_inc)
    next_total = len(inc1) + len(inc2)

    # 6) Build NEXT week's shelves.csv
    inc_layout = {
        1: make_fixed_layout(inc1, controls_per_collection),
        2: make_fixed_layout(inc2, controls_per_collection),
    }
    next_dates = calculate_dates(next_date_display)  # calculate_dates expects DD-MM-YYYY
    shelves_df = build_shelves_df(
        dates=next_dates,
        inc_layout=inc_layout,
        condition_locations=exp["condition_locations"]
    )
    shelves_csv_path = os.path.join(next_week_dir, "shelves.csv")
    shelves_df.to_csv(shelves_csv_path, index=False)

    # Determine the correct target for completion check
    if "target_replicates_total" in exp:
        target_total = int(exp["target_replicates_total"])
        fallback_note = None
    else:
        target_total = int(exp["replicates_per_experiment"])  # fallback (usually 6)
        fallback_note = "(completion target fallback to replicates_per_experiment; add 'target_replicates_total' to experiment.json for strict check)"

    # 7) Compose post-update experiment payload (state to carry forward)
    next_payload = {
        "conditions": exp["conditions"],
        "remaining": new_remaining,
        "completed_counts": new_completed_counts,              # recomputed literal N
        "replicates_per_experiment": exp["replicates_per_experiment"],
        "controls_per_collection": controls_per_collection,
        "condition_locations": exp["condition_locations"],
        "failure_counts": new_failure_counts,                  # renamed from failure_ledger
    }
    if "target_replicates_total" in exp:
        next_payload["target_replicates_total"] = target_total

    # 8) Write NEXT week's canonical experiment.json + timestamped backups
    next_exp_canonical = os.path.join(next_week_dir, "experiment.json")
    with open(next_exp_canonical, "w") as f:
        json.dump(next_payload, f, indent=4)
    next_exp_backup = write_timestamped_experiment(next_week_dir, next_payload)
    rollback_exp_path = write_timestamped_experiment(current_week_dir, next_payload)

    # 9) Update TOP-LEVEL canonical experiment.json (always current)
    top_level_exp = os.path.join(root, "experiment.json")
    with open(top_level_exp, "w") as f:
        json.dump(next_payload, f, indent=4)

    # 10) Optional next_picks
    if args.emit_next_picks:
        next_picks = {
            "week_commencing": next_date_display,  # human-friendly DD-MM-YYYY
            "per_incubator_target": per_inc,
            "incubator1": inc1,
            "incubator2": inc2,
            "picked_total": next_total,
            "new_failures_appended_this_run": newly_appended,
            "master_csv_root": root_master_path,
            "master_csv_snapshot": snapshot_path
        }
        with open(os.path.join(next_week_dir, "next_conditions.json"), "w") as f:
            json.dump(next_picks, f, indent=4)

    # 11) Prints + completion notice (based on recomputed totals)
    print(f"\nMaster (root):         {root_master_path}")
    print(f"Master snapshot (wk):  {snapshot_path}")
    print(f"Newly appended failures (delta): {sum(newly_appended.values())} across {len(newly_appended)} condition(s).")
    print(f"Next batch picked: incubator1={len(inc1)}, incubator2={len(inc2)} (target {per_inc} each).")
    print(f"NEXT week folder:      {next_week_dir}")
    print(f"  - shelves.csv:       {shelves_csv_path}")
    print(f"  - experiment.json:   {next_exp_canonical}")
    print(f"Top-level experiment.json: {top_level_exp}")
    if fallback_note:
        print(fallback_note)

    # Correct completion check (uses total target and recomputed counts)
    if all_conditions_complete(new_completed_counts, target_total, exp["conditions"]):
        print("\n✨ EXPERIMENTS COMPLETE! ✨")

if __name__ == "__main__":
    main()

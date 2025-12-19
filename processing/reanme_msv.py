import os
import re


def rename_msv_files(root_dir, start_idx, progress_signal, message_signal, pstart, total_files):
    idx = start_idx
    processed_files = pstart

    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            if fname.lower().endswith(".msv"):
                if re.match(r'^\d{5}_', fname) or "original_named_files" in dirpath:
                    print(f"Skipping already-renamed or backup file: {fname}")
                    continue

                old_path = os.path.join(dirpath, fname)

                # split at -, --, and _ with regex
                parts = re.split(r'-{1,2}|_', fname)

                year = parts[0]
                month = parts[1]
                day = parts[2]
                time = parts[3]
                type = parts[4]
                #here we skip the rest and just grab the end
                c_start = parts[-2]
                c_end = parts[-1]

                #get rid of .msv so it doesnt save .msv.msv
                c_end = c_end.replace('.msv', '')

                new_name = f"{idx:05d}_{year}_{month}_{day}__{time}_{type}_{c_start}_{c_end}.msv"
                new_path = os.path.join(dirpath, new_name)

                os.rename(old_path, new_path)
                print(f"Renamed: {fname} -> {new_name}")

                #had issue with f string converting idx to string so it was crashing when += 1
                idx = int(idx)
                idx += 1

                processed_files += 1
                progress = int((processed_files / total_files) * 100)
                progress_signal.emit(progress)
                message_signal.emit(f"Processing: {fname} -> {new_name}")
    return processed_files
import os
import subprocess
from glob import glob

def convert_mzml_to_mzxml(input_dir, output_dir, msconvert_exe_path, progress_signal, message_signal, pstart, total_files):
    # Normalize paths
    input_dir = os.path.normpath(input_dir)
    output_dir = os.path.normpath(output_dir)

    # Find all mzML files in input directory
    mzml_files = glob(os.path.join(input_dir, "*.mzML"))

    processed_files = pstart

    for mzml_file in mzml_files:
        # Get just the filename without path
        filename = os.path.basename(mzml_file)
        # Remove extension and add .mzXML
        output_filename = os.path.splitext(filename)[0] + ".mzXML"
        # Full output path in the output directory
        output_file = os.path.join(output_dir, output_filename)

        # Run msconvert command with absolute path https://proteowizard.sourceforge.io/tools/msconvert.html
        cmd = [
            msconvert_exe_path,
            mzml_file,
            "--outfile", output_file,
            "--mzXML"
        ]

        # Change working directory to output directory - msconvert saves in working directory
        original_dir = os.getcwd()
        try:
            os.chdir(output_dir)
            processed_files += 1
            progress = int((processed_files / total_files) * 100)
            progress_signal.emit(progress)
            message_signal.emit(f"Processing: {filename}")

            subprocess.run(cmd, check=True)

            # Verify file was created
            if os.path.exists(output_filename):
                print(f"Successfully saved to {output_file}")
            else:
                print(f"File not created in output directory")
        except subprocess.CalledProcessError as e:
            print(f"Error converting {mzml_file}: {e}")
        finally:
            # Always return to original directory
            os.chdir(original_dir)
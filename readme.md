# Setup and Usage Instructions

Clone with git or download zip

Download Python (tested with **3.12.6**):\
https://www.python.org/downloads/

Download and install ProteoWizard:\
https://proteowizard.sourceforge.io/

Open a terminal in the project directory and install requirements:

``` bash
pip install -r requirements.txt
```

Run the application:

``` bash
python main.py
```

------------------------------------------------------------------------

# GUI Use

The empty box at the top should contain the **absolute path** of the
ProteoWizard `msconvert.exe`.

1.  Select the `.zip` file containing `.msv` files.\
2.  Enter a **start index** to rename the files sequentially (from the
    start index to the index of the last file).\
3.  Click **Extract and Convert**, then choose an **output folder**.

# BUGS
1. Progress bar can be incorrect. Wait for the pop up of completion. 

# Considerations
1. Skips files that are not convertable only for the part that cannot be converted. 
2. Make sure zip of files has all msv files in root
# Network Merge

## Usage:
- Put all files to be merged in an "input" folder next to the application
    - For example: \
├── input \
│   ├── report1.csv \
│   └── report2.csv \
└── nmerge.exe 
- Run the nmerge.exe file
- Output will be called "output.xlsx"

## Building:

1. Clone the repository to a folder.
2. Install the required libraries: \
\
**Prerequisites**: Powershell, python, pip 
    ```
    python -m venv venv

    venv\Scripts\activate

    pip install -r requirements.txt
    ```
3. Build the program:

    ```
    python -m PyInstaller main.py --onefile
    or
    pyinstaller main.py --onefile
    ```
4. Compiled program will be in the `dist` folder.
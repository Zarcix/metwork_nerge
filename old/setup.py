from cx_Freeze import setup, Executable

# Dependencies are automatically detected, but it might need
# fine tuning.
build_options = {'packages': ["os", "numpy"], 'excludes': []}

base = 'console'

executables = [
    Executable('main.py', base=base)
]

setup(name='Metwork Nerge',
      version = '1.0',
      description = 'Merge Data Files',
      options = {'build_exe': build_options},
      executables = executables)

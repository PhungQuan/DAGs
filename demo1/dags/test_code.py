import pathlib
FOLDER_PATH = '/opt/airflow/data/northwind_gitRepo/northwind_gitRepo/tables'
all_path = [filepath for filepath in pathlib.Path(FOLDER_PATH).glob('**/*')]
    # print(filepath.absolute())
for path in all_path:
    print(path)

import os
from dotenv import load_dotenv

def get_path_from_project_root(relative_path):
    # Get the root directory of the project
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, '.'))

    # Get the absolute path of the file by joining the project root and the relative path
    file_path = os.path.join(project_root, relative_path)

    return file_path

def init_dotenv():
    # Get the path of the .env file
    env_path = get_path_from_project_root('.env')
    load_dotenv(dotenv_path=env_path)
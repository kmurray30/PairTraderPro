import os
from dotenv import load_dotenv
import glob

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

def find_files_by_pattern(directory, pattern):
    """
    Finds files in a directory matching a wildcard pattern.

    Args:
        directory: The directory to search in.
        pattern: The wildcard pattern to match (e.g., "*.txt", "image_??.png").

    Returns:
        A list of file paths that match the pattern.
    """
    
    search_path = os.path.join(directory, pattern)
    print(f"Searching for files matching {search_path}")
    matching_files = glob.glob(search_path)
    return matching_files
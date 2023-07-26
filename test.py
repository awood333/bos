import subprocess
# blob_name = 'stop1.csv' 

def run_ab(blob_name):
    try:
        # Replace 'ab.py' with the actual path to your 'azure_blob.py' script
        script_path = 'ab.py'
        command = ['python', script_path, blob_name]
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running 'azure_blob.py': {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    blob_name = 'stop1.csv'  # Replace this with the desired blob_name
    run_ab(blob_name)

import subprocess

def run_script(script_path):
    try:
        result = subprocess.run(['python', script_path], check=True, text=True, capture_output=True)
        print(f"Script {script_path} executed successfully.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Failed to execute {script_path}. Error: {e.stderr}")
        return False

def main():
    if run_script('grab_parquet.py'):
        run_script('dump_to_sql.py')
    else:
        print("Aborting second script due to failure in the first. Run them separately to debug.")

if __name__ == "__main__":
    main()

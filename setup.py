import os
import sys
import subprocess
import platform
import shutil

# Define the virtual environment directory
venv_path = os.path.join(os.getcwd(), "venv")

# Function to handle cleanup
def cleanup_environment():
    print("Cleaning up...")
    if os.path.exists(venv_path):
        shutil.rmtree(venv_path)
        print("Virtual environment removed.")

# Create the virtual environment
def create_virtual_environment():
    if os.path.exists(venv_path):
        print("Virtual environment already exists. Skipping creation.")
    else:
        print("Creating virtual environment...")
        subprocess.check_call([sys.executable, "-m", "venv", venv_path])
        print("Virtual environment created successfully.")

# Activate the virtual environment
def activate_virtual_environment():
    print("Activating virtual environment...")
    if platform.system() == "Windows":
        activate_script = os.path.join(venv_path, "Scripts", "activate")
    else:
        activate_script = os.path.join(venv_path, "bin", "activate")

    if not os.path.exists(activate_script):
        print("Activation script not found. Please check the path.")
        cleanup_environment()
        sys.exit(1)

    # The virtual environment activation (you can manually activate in the terminal, but pip installation works in the venv automatically)
    print(f"To activate the virtual environment, run:\nsource {activate_script}" if platform.system() != "Windows" else f"{activate_script}")

# Upgrade pip and install dependencies
def upgrade_pip_and_install_dependencies():
    print("Upgrading pip...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip"])

    print("Installing dependencies: discord, pyspark, and python-dotenv...")
    # Install dependencies only if not already installed
    dependencies = ['discord', 'pyspark', 'python-dotenv']

    for dependency in dependencies:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "show", dependency])
            print(f"{dependency} is already installed.")
        except subprocess.CalledProcessError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", dependency])

    print("Generating requirements.txt...")
    with open('requirements.txt', 'w') as req_file:
        subprocess.check_call([sys.executable, "-m", "pip", "freeze"], stdout=req_file)
    print("requirements.txt generated.")

if __name__ == "__main__":
    try:
        create_virtual_environment()
        activate_virtual_environment()  # Activation message for the user
        upgrade_pip_and_install_dependencies()
        print("Setup complete. Virtual environment is ready and requirements.txt generated.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        cleanup_environment()

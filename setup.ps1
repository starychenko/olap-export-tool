# setup_eng.ps1
# Script for automating the creation of a Python virtual environment and installing dependencies
# Author: Claude AI
# Date: 2023

# Function for displaying colored text
function Write-ColorOutput {
    param (
        [string]$Message,
        [string]$Color = "White"
    )
    Write-Host $Message -ForegroundColor $Color
}

# Setting strict error handling
$ErrorActionPreference = "Stop"

# Header
Write-ColorOutput "===== Setting up virtual environment for OLAP Export Tool =====" "Cyan"
Write-ColorOutput "This script will create a Python virtual environment and install all necessary dependencies." "Yellow"

# Check for Python with improved error handling
try {
    $pythonVersion = python --version
    Write-ColorOutput "Found $pythonVersion" "Green"
}
catch {
    Write-ColorOutput "Python not found or incorrectly configured. Please install Python 3.8 or newer." "Red"
    Write-ColorOutput "Error details: $_" "Red"
    exit 1
}

# Check for requirements.txt file
if (-not (Test-Path -Path ".\requirements.txt")) {
    Write-ColorOutput "Error: requirements.txt file not found in the current directory." "Red"
    Write-ColorOutput "Please make sure you are in the correct project directory or create a requirements.txt file." "Red"
    exit 1
}

# Check if virtual environment exists
if (Test-Path -Path ".\venv") {
    Write-ColorOutput "Existing virtual environment detected." "Yellow"
    $confirmation = Read-Host "Do you want to use the existing environment (Y), delete and create a new one (R), or cancel the operation (N)? [Y/R/N]"
    
    if ($confirmation -eq "N") {
        Write-ColorOutput "Operation cancelled." "Red"
        exit 0
    }
    elseif ($confirmation -eq "R") {
        Write-ColorOutput "Deleting existing virtual environment..." "Yellow"
        try {
            # Make sure no processes are using the directory
            if (Get-Process | Where-Object { $_.Path -like "*\venv\*" }) {
                Write-ColorOutput "Warning: Some processes are using files in the virtual environment." "Red"
                Write-ColorOutput "Please close all programs that are using these files and try again." "Red"
                exit 1
            }
            Remove-Item -Recurse -Force ".\venv"
            Write-ColorOutput "Old virtual environment deleted." "Green"
        }
        catch {
            Write-ColorOutput "Error deleting virtual environment: $_" "Red"
            Write-ColorOutput "Please close all programs that are using these files and try again." "Red"
            exit 1
        }
    }
    else {
        Write-ColorOutput "Using existing virtual environment." "Green"
        
        # Check if all dependencies are installed
        $checkDeps = Read-Host "Do you want to reinstall dependencies in the existing environment? [Y/N]"
        if ($checkDeps -eq "Y") {
            Write-ColorOutput "Activating virtual environment..." "Blue"
            try {
                # Activate the virtual environment
                & .\venv\Scripts\Activate.ps1
                
                # Check if activation was successful
                if (-not $env:VIRTUAL_ENV) {
                    throw "Failed to activate virtual environment"
                }
                
                Write-ColorOutput "Installing dependencies..." "Blue"
                # Install dependencies with error handling
                $pipResult = pip install -r requirements.txt
                if ($LASTEXITCODE -ne 0) {
                    throw "Failed to install dependencies"
                }
                
                Write-ColorOutput "Dependencies successfully installed!" "Green"
                
                # Deactivate the environment if the function exists
                if (Get-Command deactivate -ErrorAction SilentlyContinue) {
                    deactivate
                } else {
                    # Alternative method of deactivation if the function is not available
                    Write-ColorOutput "Note: Standard deactivate function not found, clearing environment variables..." "Yellow"
                    $env:VIRTUAL_ENV = $null
                    # Remove the Scripts directory of the virtual environment from PATH
                    $env:PATH = ($env:PATH -split ';' | Where-Object { $_ -notlike "*\venv\Scripts*" }) -join ';'
                }
                
                exit 0
            }
            catch {
                Write-ColorOutput "Error: $_" "Red"
                exit 1
            }
        }
        else {
            Write-ColorOutput "Operation completed. To activate the environment use: .\venv\Scripts\Activate.ps1" "Cyan"
            exit 0
        }
    }
}

# Creating virtual environment
Write-ColorOutput "Creating Python virtual environment..." "Blue"
try {
    python -m venv venv

    if (-not (Test-Path -Path ".\venv")) {
        throw "Virtual environment directory not created"
    }
}
catch {
    Write-ColorOutput "Failed to create virtual environment." "Red"
    Write-ColorOutput "Error details: $_" "Red"
    Write-ColorOutput "Check if the 'venv' package is installed. Try: python -m pip install virtualenv" "Yellow"
    exit 1
}

Write-ColorOutput "Virtual environment successfully created!" "Green"

# Activating virtual environment and installing dependencies
Write-ColorOutput "Activating virtual environment..." "Blue"
try {
    # Activate the virtual environment
    & .\venv\Scripts\Activate.ps1
    
    # Check if activation was successful
    if (-not $env:VIRTUAL_ENV) {
        throw "Failed to activate virtual environment"
    }
    
    # Install dependencies
    Write-ColorOutput "Installing dependencies from requirements.txt..." "Blue"
    $pipResult = pip install -r requirements.txt
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to install dependencies"
    }
}
catch {
    Write-ColorOutput "Error: $_" "Red"
    exit 1
}

# Check for .env file
if (-not (Test-Path -Path ".\.env")) {
    if (Test-Path -Path ".\env.example") {
        Write-ColorOutput ".env file not found. Creating it from env.example..." "Yellow"
        try {
            Copy-Item -Path ".\env.example" -Destination ".\.env"
            Write-ColorOutput ".env file created. Don't forget to configure it according to your environment!" "Yellow"
        }
        catch {
            Write-ColorOutput "Error creating .env file: $_" "Red"
        }
    }
    else {
        Write-ColorOutput "Warning: .env and env.example files not found. You need to create an .env file manually." "Red"
    }
}

# Conclusion
Write-ColorOutput "===== Setup completed! =====" "Cyan"
Write-ColorOutput "Virtual environment created and activated." "Green"
Write-ColorOutput "All dependencies installed." "Green"
Write-ColorOutput "" "White"
Write-ColorOutput "To activate the environment use: .\venv\Scripts\Activate.ps1" "Yellow"
Write-ColorOutput "To deactivate the environment use the command: deactivate" "Yellow"

# Ask if the user wants to keep the environment activated
$keepActive = Read-Host "Do you want to keep the virtual environment activated? [Y/N]"
if ($keepActive -ne "Y") {
    Write-ColorOutput "Deactivating virtual environment..." "Blue"
    # Deactivate the environment if the function exists
    if (Get-Command deactivate -ErrorAction SilentlyContinue) {
        deactivate
    } else {
        # Alternative method of deactivation if the function is not available
        Write-ColorOutput "Note: Standard deactivate function not found, clearing environment variables..." "Yellow"
        $env:VIRTUAL_ENV = $null
        # Remove the Scripts directory of the virtual environment from PATH
        $env:PATH = ($env:PATH -split ';' | Where-Object { $_ -notlike "*\venv\Scripts*" }) -join ';'
    }
    Write-ColorOutput "Virtual environment deactivated." "Green"
}
else {
    Write-ColorOutput "Virtual environment remains activated." "Green"
} 
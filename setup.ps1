# setup.ps1
# Universal setup script for OLAP Export Tool
# Checks Python version, installs Python 3.13 if needed, creates venv, installs dependencies

function Write-ColorOutput {
    param (
        [string]$Message,
        [string]$Color = "White"
    )
    Write-Host $Message -ForegroundColor $Color
}

$ErrorActionPreference = "Stop"

Write-ColorOutput "===== OLAP Export Tool - Setup =====" "Cyan"
Write-ColorOutput "This script will set up Python environment and dependencies" "Yellow"
Write-ColorOutput "" "White"

# Check if requirements.txt exists
if (-not (Test-Path -Path ".\requirements.txt")) {
    Write-ColorOutput "Error: requirements.txt file not found in current directory." "Red"
    Write-ColorOutput "Please make sure you are in the correct project directory." "Red"
    exit 1
}

# Check current Python version
Write-ColorOutput "Checking Python installation..." "Blue"
$pythonExe = $null
$needInstall = $false

# Try py launcher first
try {
    $pyLauncherCheck = py -3.13 --version 2>&1
    if ($pyLauncherCheck -match "Python 3\.13") {
        $pythonExe = "py -3.13"
        Write-ColorOutput "Found Python 3.13 via py launcher: $pyLauncherCheck" "Green"
    }
} catch {
}

# Try python command if py launcher didn't work
if (-not $pythonExe) {
    try {
        $pythonCheck = python --version 2>&1
        if ($pythonCheck -match "Python 3\.13") {
            $pythonExe = "python"
            Write-ColorOutput "Found Python 3.13: $pythonCheck" "Green"
        } elseif ($pythonCheck -match "Python 3\.(8|9|10|11|12)") {
            Write-ColorOutput "Found Python: $pythonCheck" "Yellow"
            Write-ColorOutput "Python 3.13 is recommended for this project" "Yellow"
            $upgrade = Read-Host "Install Python 3.13? [Y/N]"
            if ($upgrade -eq "Y") {
                $needInstall = $true
            } else {
                Write-ColorOutput "Continuing with current Python version..." "Yellow"
                $pythonExe = "python"
            }
        } elseif ($pythonCheck -match "Python 3\.14") {
            Write-ColorOutput "Found Python 3.14 - NOT SUPPORTED!" "Red"
            Write-ColorOutput "Python 3.14 has compatibility issues with pythonnet" "Red"
            Write-ColorOutput "Python 3.13 will be installed instead" "Yellow"
            $needInstall = $true
        } else {
            Write-ColorOutput "Found Python: $pythonCheck" "Yellow"
            Write-ColorOutput "Python 3.13 is required for this project" "Yellow"
            $needInstall = $true
        }
    } catch {
        Write-ColorOutput "Python not found" "Yellow"
        $needInstall = $true
    }
}

# Install Python 3.13 if needed
if ($needInstall) {
    $PYTHON_VERSION = "3.13.8"
    $PYTHON_URL = "https://www.python.org/ftp/python/$PYTHON_VERSION/python-$PYTHON_VERSION-amd64.exe"
    $INSTALLER_PATH = "$env:TEMP\python-$PYTHON_VERSION-amd64.exe"

    Write-ColorOutput "" "White"
    Write-ColorOutput "Installing Python $PYTHON_VERSION..." "Cyan"
    Write-ColorOutput "Installation location: %LOCALAPPDATA%\Programs\Python\Python313\" "Gray"
    Write-ColorOutput "" "White"

    # Download Python
    Write-ColorOutput "Downloading Python $PYTHON_VERSION..." "Blue"
    Write-ColorOutput "URL: $PYTHON_URL" "Gray"

    try {
        $ProgressPreference = 'SilentlyContinue'
        Invoke-WebRequest -Uri $PYTHON_URL -OutFile $INSTALLER_PATH -UseBasicParsing
        $ProgressPreference = 'Continue'
        Write-ColorOutput "Download completed" "Green"
    } catch {
        Write-ColorOutput "Download error: $_" "Red"
        Write-ColorOutput "" "White"
        Write-ColorOutput "Alternative: Download manually from:" "Yellow"
        Write-ColorOutput "https://www.python.org/downloads/release/python-$PYTHON_VERSION/" "Cyan"
        exit 1
    }

    # Install Python
    Write-ColorOutput "" "White"
    Write-ColorOutput "Installing Python $PYTHON_VERSION..." "Blue"
    Write-ColorOutput "This may take a few minutes..." "Yellow"

    try {
        $installArgs = @(
            "/quiet",
            "InstallAllUsers=0",
            "PrependPath=1",
            "Include_test=0",
            "Include_pip=1",
            "Include_launcher=1"
        )

        $process = Start-Process -FilePath $INSTALLER_PATH -ArgumentList $installArgs -Wait -PassThru

        if ($process.ExitCode -ne 0) {
            throw "Installer failed with exit code: $($process.ExitCode)"
        }

        Write-ColorOutput "Python $PYTHON_VERSION successfully installed!" "Green"
        Remove-Item -Path $INSTALLER_PATH -Force -ErrorAction SilentlyContinue

        # Refresh environment variables
        Write-ColorOutput "Refreshing environment variables..." "Blue"
        $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "User") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "Machine")
        Start-Sleep -Seconds 3

        # Set pythonExe
        try {
            py -3.13 --version | Out-Null
            $pythonExe = "py -3.13"
        } catch {
            $pythonExe = "python"
        }

    } catch {
        Write-ColorOutput "Python installation error: $_" "Red"
        Write-ColorOutput "" "White"
        Write-ColorOutput "Try installing manually:" "Yellow"
        Write-ColorOutput "1. Run installer: $INSTALLER_PATH" "Yellow"
        Write-ColorOutput "2. Check 'Add Python to PATH'" "Yellow"
        Write-ColorOutput "3. Run this script again" "Yellow"
        exit 1
    }
}

# Remove old venv if exists
Write-ColorOutput "" "White"
if (Test-Path -Path ".\venv") {
    Write-ColorOutput "Found existing virtual environment" "Yellow"
    $recreate = Read-Host "Recreate virtual environment? [Y/N]"

    if ($recreate -eq "Y") {
        Write-ColorOutput "Removing old virtual environment..." "Blue"
        try {
            Remove-Item -Recurse -Force ".\venv"
            Write-ColorOutput "Old virtual environment removed" "Green"
        } catch {
            Write-ColorOutput "Error removing virtual environment: $_" "Red"
            Write-ColorOutput "Close all programs using venv and try again" "Yellow"
            exit 1
        }
    } else {
        Write-ColorOutput "Keeping existing virtual environment" "Green"
        Write-ColorOutput "Will only update dependencies..." "Yellow"

        # Update dependencies in existing venv
        Write-ColorOutput "" "White"
        Write-ColorOutput "Updating dependencies..." "Blue"
        try {
            $pipPath = ".\venv\Scripts\python.exe"
            & $pipPath -m pip install --upgrade pip --quiet
            & $pipPath -m pip install -r requirements.txt

            if ($LASTEXITCODE -ne 0) {
                throw "Error installing dependencies"
            }

            Write-ColorOutput "Dependencies updated!" "Green"
        } catch {
            Write-ColorOutput "Error updating dependencies: $_" "Red"
            exit 1
        }

        # Skip to final message
        Write-ColorOutput "" "White"
        Write-ColorOutput "===== Setup Complete! =====" "Cyan"
        Write-ColorOutput "" "White"
        Write-ColorOutput "To run the program:" "Yellow"
        Write-ColorOutput "  .\venv\Scripts\python.exe olap.py" "Cyan"
        Write-ColorOutput "" "White"

        $runNow = Read-Host "Run program now? [Y/N]"
        if ($runNow -eq "Y") {
            Write-ColorOutput "" "White"
            Write-ColorOutput "Starting program..." "Blue"
            Write-ColorOutput "========================================" "Cyan"
            & .\venv\Scripts\python.exe olap.py
        }
        exit 0
    }
}

# Create new virtual environment
Write-ColorOutput "" "White"
Write-ColorOutput "Creating virtual environment..." "Blue"
try {
    if ($pythonExe -eq "py -3.13") {
        & py -3.13 -m venv venv
    } else {
        & python -m venv venv
    }

    if (-not (Test-Path -Path ".\venv")) {
        throw "Virtual environment was not created"
    }

    Write-ColorOutput "Virtual environment created!" "Green"
} catch {
    Write-ColorOutput "Error creating virtual environment: $_" "Red"
    exit 1
}

# Install dependencies
Write-ColorOutput "" "White"
Write-ColorOutput "Installing dependencies from requirements.txt..." "Blue"
Write-ColorOutput "This may take a few minutes..." "Yellow"

try {
    $pipPath = ".\venv\Scripts\python.exe"

    Write-ColorOutput "Upgrading pip..." "Blue"
    & $pipPath -m pip install --upgrade pip --quiet

    Write-ColorOutput "Installing packages..." "Blue"
    & $pipPath -m pip install -r requirements.txt

    if ($LASTEXITCODE -ne 0) {
        throw "Error installing dependencies"
    }

    Write-ColorOutput "All dependencies installed!" "Green"

} catch {
    Write-ColorOutput "Error installing dependencies: $_" "Red"
    exit 1
}

# Create .env if needed
if (-not (Test-Path -Path ".\.env")) {
    if (Test-Path -Path ".\.env.example") {
        Write-ColorOutput "" "White"
        Write-ColorOutput ".env file not found. Creating from .env.example..." "Yellow"
        try {
            Copy-Item -Path ".\.env.example" -Destination ".\.env"
            Write-ColorOutput ".env file created. Configure it according to your environment!" "Yellow"
        } catch {
            Write-ColorOutput "Error creating .env file: $_" "Red"
        }
    } else {
        Write-ColorOutput "" "White"
        Write-ColorOutput "Warning: .env and .env.example files not found." "Yellow"
        Write-ColorOutput "You need to create .env file manually." "Yellow"
    }
}

# Show installed packages
Write-ColorOutput "" "White"
Write-ColorOutput "Installed packages:" "Blue"
Write-ColorOutput "----------------------------------------" "Gray"
& .\venv\Scripts\python.exe -m pip list
Write-ColorOutput "----------------------------------------" "Gray"

# Final message
Write-ColorOutput "" "White"
Write-ColorOutput "===== Setup Complete! =====" "Cyan"
Write-ColorOutput "" "White"
Write-ColorOutput "Environment successfully configured!" "Green"
Write-ColorOutput "" "White"
Write-ColorOutput "To run the program:" "Yellow"
Write-ColorOutput "  1. Activate virtual environment: .\venv\Scripts\Activate.ps1" "Cyan"
Write-ColorOutput "  2. Run program: python olap.py" "Cyan"
Write-ColorOutput "" "White"
Write-ColorOutput "Or run without activation:" "Yellow"
Write-ColorOutput "  .\venv\Scripts\python.exe olap.py" "Cyan"
Write-ColorOutput "" "White"

# Offer to run now
$runNow = Read-Host "Run program now? [Y/N]"
if ($runNow -eq "Y") {
    Write-ColorOutput "" "White"
    Write-ColorOutput "Starting program..." "Blue"
    Write-ColorOutput "========================================" "Cyan"
    & .\venv\Scripts\python.exe olap.py
}

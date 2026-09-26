$ErrorActionPreference = "Stop"

function Invoke-Docker {
    & docker @args
    if ($LASTEXITCODE -ne 0) {
        throw "Docker command failed. Check the output above."
    }
}

Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw "Install Docker Desktop with Docker Compose, then run this script again."
    }
    & docker info *> $null
    if ($LASTEXITCODE -ne 0) {
        throw "Start Docker Desktop (Linux containers), then run this script again."
    }
    Invoke-Docker compose version
    if (-not (Test-Path -LiteralPath ".env")) {
        Copy-Item -LiteralPath ".env.example" -Destination ".env"
        Write-Host "Created .env from .env.example."
    }
    Invoke-Docker compose build api
    Invoke-Docker compose up -d --wait
    $apiPort = Invoke-Docker compose exec -T api python -c "from src.settings.config import get_settings; print(get_settings().server_port)"
    $address = Invoke-Docker compose port api $apiPort
    Write-Host "App: http://$address"
    Write-Host "API docs: http://$address/docs"
    Write-Host "The first transcription downloads and loads the model."
}
catch {
    Write-Host "Startup failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
finally {
    Pop-Location
}

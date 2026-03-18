# ============================================================================
# Script de Evaluación - Entrevista Técnica Data Engineer
# ============================================================================
# Ejecuta el pipeline completo ETL para evaluación del candidato
# Prerequisito: data_jobs.csv debe estar en el directorio raíz
# ============================================================================

$ErrorActionPreference = "Stop"
$StartTime = Get-Date

Write-Host "`n=== EVALUACIÓN TÉCNICA - DATA ENGINEER ===" -ForegroundColor Cyan
Write-Host "Inicio: $($StartTime.ToString('yyyy-MM-dd HH:mm:ss'))`n" -ForegroundColor Gray

# Verificar prerequisitos
Write-Host "Verificando prerequisitos..." -ForegroundColor Yellow

if (-not (Test-Path "data_jobs.csv")) {
    Write-Host "ERROR: data_jobs.csv no encontrado en el directorio raíz" -ForegroundColor Red
    exit 1
}
Write-Host "✓ data_jobs.csv encontrado" -ForegroundColor Green

# Verificar Poetry
try {
    poetry --version >$null 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Poetry no está instalado" -ForegroundColor Red
        Write-Host "Instala Poetry desde: https://python-poetry.org/docs/#installation" -ForegroundColor Yellow
        exit 1
    }
    Write-Host "✓ Poetry está instalado" -ForegroundColor Green
} catch {
    Write-Host "ERROR: Poetry no está instalado" -ForegroundColor Red
    exit 1
}

# Verificar Docker
try {
    docker ps >$null 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Docker no está corriendo. Por favor inicia Docker Desktop" -ForegroundColor Red
        exit 1
    }
    Write-Host "✓ Docker está corriendo" -ForegroundColor Green
} catch {
    Write-Host "ERROR: Docker no está instalado" -ForegroundColor Red
    exit 1
}

Write-Host "`n--- INICIANDO PIPELINE ---`n" -ForegroundColor Cyan

# 0. Iniciar PostgreSQL
Write-Host "[0/6] Iniciando PostgreSQL..." -ForegroundColor Yellow
docker compose up -d db
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host "Esperando a que PostgreSQL esté listo..." -ForegroundColor Gray
$ready = $false
for ($i = 1; $i -le 30; $i++) {
    docker compose exec -T db pg_isready -U postgres >$null 2>&1
    if ($LASTEXITCODE -eq 0) {
        $ready = $true
        break
    }
    Start-Sleep -Seconds 2
}

if (-not $ready) {
    Write-Host "ERROR: PostgreSQL no respondió" -ForegroundColor Red
    exit 1
}
Write-Host "✓ PostgreSQL listo`n" -ForegroundColor Green

# 1. Extracción
Write-Host "[1/6] Ejecutando extracción de datos..." -ForegroundColor Yellow
poetry run python main.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Falló la extracción. Ver logs/extraction.log" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Extracción completada`n" -ForegroundColor Green

# 2. Tests de extracción
Write-Host "[2/6] Ejecutando tests de extracción..." -ForegroundColor Yellow
poetry run pytest tests/test_extraction.py -v
if ($LASTEXITCODE -ne 0) {
    Write-Host "ADVERTENCIA: Algunos tests de extracción fallaron" -ForegroundColor Yellow
}
Write-Host "✓ Tests de extracción completados`n" -ForegroundColor Green

# 3. Configurar dbt
Write-Host "[3/6] Configurando dbt (profiles.yml)..." -ForegroundColor Yellow
poetry run python utils/setup_dbt_profile.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Falló la configuración de dbt" -ForegroundColor Red
    exit 1
}
Write-Host "✓ dbt configurado`n" -ForegroundColor Green

# 4. Transformaciones dbt
Write-Host "[4/6] Ejecutando transformaciones dbt..." -ForegroundColor Yellow
Set-Location datajob_etl
poetry run dbt run
$dbtRunResult = $LASTEXITCODE
Set-Location ..

if ($dbtRunResult -ne 0) {
    Write-Host "ERROR: Fallaron las transformaciones dbt" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Transformaciones completadas`n" -ForegroundColor Green

# 5. Tests de transformaciones
Write-Host "[5/6] Ejecutando tests de transformaciones..." -ForegroundColor Yellow
Set-Location datajob_etl
poetry run dbt test
$dbtTestResult = $LASTEXITCODE
Set-Location ..

if ($dbtTestResult -ne 0) {
    Write-Host "ADVERTENCIA: Algunos tests de transformaciones fallaron" -ForegroundColor Yellow
}
Write-Host "✓ Tests de transformaciones completados`n" -ForegroundColor Green

# 6. Tests de calidad de datos
Write-Host "[6/6] Ejecutando tests de calidad de datos..." -ForegroundColor Yellow
Set-Location datajob_etl
poetry run dbt test --select test_type:generic
poetry run dbt test --select test_type:singular
Set-Location ..
Write-Host "✓ Tests de calidad completados`n" -ForegroundColor Green

# Resumen
$EndTime = Get-Date
$Duration = $EndTime - $StartTime
Write-Host "`n=== EVALUACIÓN COMPLETADA ===" -ForegroundColor Green
Write-Host "Duración total: $([math]::Round($Duration.TotalMinutes, 2)) minutos" -ForegroundColor Gray
Write-Host "`nLogs disponibles en:" -ForegroundColor White
Write-Host "  • logs/pipeline.log" -ForegroundColor Gray
Write-Host "  • logs/extraction.log" -ForegroundColor Gray
Write-Host "  • datajob_etl/logs/dbt.log" -ForegroundColor Gray
Write-Host "`nPara detener PostgreSQL: docker compose down`n" -ForegroundColor Gray

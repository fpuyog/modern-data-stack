# AWS Deployment - Step by Step Guide
# Guía paso a paso para desplegar el proyecto en AWS

## IMPORTANTE: Sigue estos pasos en orden

### Paso 1: Instalar AWS CLI (si no funciona el comando 'aws')

Si acabas de instalar AWS CLI, necesitas:
1. Cerrar y volver a abrir PowerShell/Terminal
2. O agregar AWS CLI al PATH manualmente

**Verificar instalación:**
```powershell
aws --version
```

Si no funciona, busca AWS CLI en:
```
C:\Program Files\Amazon\AWSCLIV2\aws.exe
```

### Paso 2: Instalar Python Dependencies

```powershell
# Activar virtual environment (si no está activo)
.\venv\Scripts\activate

# Instalar requirements
pip install -r requirements.txt
```

### Paso 3: Configurar AWS Credentials

Necesitas tener:
- AWS Access Key ID
- AWS Secret Access Key

**Obtener credenciales:**
1. Ir a AWS Console: https://console.aws.amazon.com
2. Ir a IAM → Users → Tu usuario → Security credentials
3. Crear "Access key" si no tienes

**Configurar:**
```powershell
aws configure
```

Cuando te pida:
- AWS Access Key ID: [TU_ACCESS_KEY]
- AWS Secret Access Key: [TU_SECRET_KEY]
- Default region name: us-east-1
- Default output format: json

### Paso 4: Crear S3 Buckets

```powershell
# Ejecutar el script de creación de buckets
python scripts/setup_aws_buckets.py
```

O manualmente:
```powershell
aws s3 mb s3://modern-data-stack-bronze
aws s3 mb s3://modern-data-stack-silver
aws s3 mb s3://modern-data-stack-gold
aws s3 mb s3://modern-data-stack-athena-results
```

**NOTA**: Los nombres de buckets S3 deben ser únicos globalmente.
Si estos nombres ya están tomados, usa:
- s3://tu-nombre-modern-data-stack-bronze
- s3://tu-nombre-modern-data-stack-silver
- etc.

### Paso 5: Configurar API Keys

#### OpenWeatherMap (FREE)
1. Ir a: https://openweathermap.org/api
2. Sign up → Get API Key (gratis)
3. Copiar el API key

#### Alpha Vantage (FREE)
1. Ir a: https://www.alphavantage.co/support/#api-key
2. Get your free API key
3. Copiar el API key

### Paso 6: Editar Archivos de Configuración

**Archivo: config/database_config.yaml**
```yaml
postgres:
  local:
    host: localhost
    port: 5432
    database: airflow
    username: airflow
    password: airflow

apis:
  openweathermap:
    api_key: TU_OPENWEATHERMAP_API_KEY_AQUI
    base_url: https://api.openweathermap.org/data/2.5
    cities:
      - "New York"
      - "London"
      - "Tokyo"
      - "Buenos Aires"
  
  alphavantage:
    api_key: TU_ALPHAVANTAGE_API_KEY_AQUI
    base_url: https://www.alphavantage.co/query
    symbols:
      - "AAPL"
      - "GOOGL"
      - "MSFT"
```

**Archivo: config/aws_config.yaml**
```yaml
aws:
  region: us-east-1
  profile: default

s3:
  bronze_bucket: modern-data-stack-bronze  # O tu nombre personalizado
  silver_bucket: modern-data-stack-silver
  gold_bucket: modern-data-stack-gold
  
  prefixes:
    weather: weather/
    stocks: stocks/
    transactions: transactions/
    users: users/

athena:
  database: modern_data_warehouse
  workgroup: primary
  output_location: s3://modern-data-stack-athena-results/

lambda:
  function_name: modern-data-stack-trigger
  role_arn: arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-execution-role
```

### Paso 7: Ejecutar el Pipeline

#### 7.1. Ingestión de Datos

```powershell
# Weather data
python src/ingestion/weather_api_ingestion.py

# Stock data (NOTA: Tarda ~1 minuto por las limitaciones de API)
python src/ingestion/stock_api_ingestion.py

# CSV data
python src/ingestion/csv_ingestion.py
```

#### 7.2. Verificar datos en S3

```powershell
aws s3 ls s3://modern-data-stack-bronze/weather/ --recursive
aws s3 ls s3://modern-data-stack-bronze/stocks/ --recursive
```

#### 7.3. Transformaciones (Opcional - requiere PySpark configurado)

```powershell
# Bronze to Silver
python src/transformation/bronze_to_silver.py

# Silver to Gold
python src/transformation/silver_to_gold.py
```

#### 7.4. Consultas en Athena

1. Ir a AWS Athena Console
2. Copiar y ejecutar: `sql/athena_table_creation.sql`
3. Ejecutar queries de: `sql/analytical_queries.sql`

### Paso 8: Verificar Costos

```powershell
# Verificar que estás en free tier
aws ce get-cost-and-usage --time-period Start=2024-12-01,End=2024-12-31 --granularity MONTHLY --metrics BlendedCost
```

## Troubleshooting

### Error: "No module named boto3"
```powershell
pip install boto3
```

### Error: "aws: command not found"
- Reiniciar terminal
- O usar path completo: `C:\Program Files\Amazon\AWSCLIV2\aws.exe`

### Error: "Access Denied" en S3
- Verificar credenciales: `aws sts get-caller-identity`
- Verificar permisos IAM

### Error: "Bucket already exists"
- Cambiar nombre del bucket a algo único
- Actualizar config/aws_config.yaml

## Siguiente Paso

Una vez completados estos pasos, ejecuta:
```powershell
python scripts/run_pipeline.py
```

Este script ejecutará todo el pipeline automáticamente.

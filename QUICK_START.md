# CONFIGURACIÓN PASO A PASO - DEPLOYMENT AWS

## ⚡ INICIO RÁPIDO

Sigue estos 4 pasos para tener todo corriendo:

### 1️⃣ Instalar Dependencias

```powershell
# Asegúrate de estar en la carpeta del proyecto
cd C:\Users\Checo\.gemini\antigravity\scratch\modern-data-stack

# Opción A: Instalar dependencias mínimas (RECOMENDADO para empezar)
pip install -r requirements-minimal.txt

# Opción B: Instalar todo incluyendo Airflow (si quieres Airflow local)
# NOTA: Airflow ya está en Docker, no es necesario instalarlo en Python
# pip install -r requirements.txt
```

### 2️⃣ Configurar AWS CLI

Si AWS CLI ya está instalado pero no funciona el comando, necesitas reiniciar PowerShell o agregarlo al PATH.

**Configurar credenciales:**
```powershell
# Método 1: Usar AWS CLI
aws configure

# Te preguntará:
# AWS Access Key ID [None]: TU_ACCESS_KEY
# AWS Secret Access Key [None]: TU_SECRET_KEY
# Default region name [None]: us-east-1
# Default output format [None]: json
```

**¿No tienes Access Keys?**
1. Ir a AWS Console: https://console.aws.amazon.com/iam/
2. IAM → Users → Tu usuario → Security credentials
3. Click "Create access key"
4. Descargar las credenciales

Al terminar, verificar:
```powershell
python scripts/test_aws_connection.py
```

### 3️⃣ Crear Buckets S3

```powershell
python scripts/setup_aws_buckets.py
```

Este script:
- ✅ Verifica tus credenciales AWS
- ✅ Crea 4 buckets S3 automáticamente
- ✅ Verifica que todo funcione

### 4️⃣ Configurar API Keys

#### A. OpenWeatherMap (GRATIS)
1. https://openweathermap.org/api → Sign up
2. Copiar API key
3. Editar: `config/database_config.yaml.example`
   - Cambiar `YOUR_API_KEY_HERE` por tu API key real
4. Guardar como: `config/database_config.yaml` (sin .example)

#### B. Alpha Vantage (GRATIS)
1. https://www.alphavantage.co/support/#api-key
2. Copiar API key
3. Editar el mismo archivo
4. Cambiar el segundo `YOUR_API_KEY_HERE`

#### C. Editar AWS Config
1. Editar: `config/aws_config.yaml.example`
2. Verificar que los nombres de buckets coincidan con los que creaste
3. Guardar como: `config/aws_config.yaml`

---

## 🚀 EJECUTAR EL PIPELINE

Una vez completados los 4 pasos anteriores:

```powershell
python scripts/run_pipeline.py
```

Esto ejecutará:
1. ✅ Ingestion de datos del clima (OpenWeatherMap)
2. ✅ Ingestion de datos de stocks (Alpha Vantage)
3. ✅ Ingestion de datos de CSV

---

## ✅ VERIFICAR RESULTADOS

### Ver datos en S3:
```powershell
aws s3 ls s3://modern-data-stack-bronze/weather/ --recursive
aws s3 ls s3://modern-data-stack-bronze/stocks/ --recursive
aws s3 ls s3://modern-data-stack-bronze/transactions/ --recursive
```

### Consultar en Athena:
1. Ir a: https://console.aws.amazon.com/athena/
2. Copiar el contenido de: `sql/athena_table_creation.sql`
3. Ejecutar en Athena
4. Ahora puedes hacer queries SQL:
```sql
SELECT * FROM aggregated_weather_metrics LIMIT 10;
```

---

## 🐛 SOLUCIÓN DE PROBLEMAS

### "aws: command not found"
```powershell
# Opción 1: Reiniciar PowerShell
# Opción 2: Usar ruta completa
C:\"Program Files"\Amazon\AWSCLIV2\aws.exe --version
```

### "No module named 'boto3'"
```powershell
pip install boto3
```

### "The specified bucket does not exist"
```powershell
# Crear buckets manualmente
aws s3 mb s3://modern-data-stack-bronze
aws s3 mb s3://modern-data-stack-silver
aws s3 mb s3://modern-data-stack-gold
```

### "Invalid API key"
- Verificar que copiaste bien las API keys en `config/database_config.yaml`
- Asegurarte de guardar el archivo SIN la extensión `.example`

---

## 📊 COSTOS

- **S3**: Gratis (dentro de 5GB)
- **Athena**: ~$0.01 por ejecución
- **Lambda**: No usado aún (gratis)
- **Total estimado**: $0 - $2/mes

---

## 🎯 SIGUIENTE NIVEL (Opcional)

Una vez que tengas datos en S3:

### Ejecutar transformaciones PySpark:
```powershell
python src/transformation/bronze_to_silver.py
python src/transformation/silver_to_gold.py
```

### Iniciar Airflow localmente:
```powershell
docker-compose up -d
# Acceder en: http://localhost:8080
# User: admin / Pass: admin
```

---

## 📞 AYUDA

Si tienes problemas:
1. Revisar DEPLOYMENT_GUIDE.md para más detalles
2. Ejecutar: `python scripts/test_aws_connection.py`
3. Verificar logs en carpeta `logs/`

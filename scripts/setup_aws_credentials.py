"""
AWS Credentials Setup Script
Configure AWS credentials without using AWS CLI
"""

import os
from pathlib import Path


def setup_aws_credentials():
    """Setup AWS credentials manually"""
    print("=" * 70)
    print("  AWS CREDENTIALS SETUP")
    print("=" * 70)
    print()
    print("Este script te ayudará a configurar tus credenciales de AWS")
    print("sin necesidad del comando 'aws configure'")
    print()
    
    # Get credentials from user
    print("Por favor ingresa tus credenciales de AWS:")
    print("-" * 70)
    
    access_key = input("AWS Access Key ID: ").strip()
    if not access_key:
        print("✗ Access Key ID es requerido")
        return False
    
    secret_key = input("AWS Secret Access Key: ").strip()
    if not secret_key:
        print("✗ Secret Access Key es requerido")
        return False
    
    region = input("AWS Region [us-east-1]: ").strip() or "us-east-1"
    output = input("Output format [json]: ").strip() or "json"
    
    print()
    print("-" * 70)
    
    # Create .aws directory
    aws_dir = Path.home() / '.aws'
    aws_dir.mkdir(exist_ok=True)
    print(f"✓ Carpeta creada: {aws_dir}")
    
    # Create credentials file
    credentials_file = aws_dir / 'credentials'
    credentials_content = f"""[default]
aws_access_key_id = {access_key}
aws_secret_access_key = {secret_key}
"""
    
    with open(credentials_file, 'w') as f:
        f.write(credentials_content)
    
    print(f"✓ Archivo de credenciales creado: {credentials_file}")
    
    # Create config file
    config_file = aws_dir / 'config'
    config_content = f"""[default]
region = {region}
output = {output}
"""
    
    with open(config_file, 'w') as f:
        f.write(config_content)
    
    print(f"✓ Archivo de configuración creado: {config_file}")
    
    print()
    print("=" * 70)
    print("✓ AWS Credentials configuradas exitosamente!")
    print("=" * 70)
    print()
    print("Verifica la configuración ejecutando:")
    print("  python scripts/test_aws_connection.py")
    print()
    
    return True


def show_existing_credentials():
    """Show if credentials already exist"""
    aws_dir = Path.home() / '.aws'
    credentials_file = aws_dir / 'credentials'
    config_file = aws_dir / 'config'
    
    if credentials_file.exists():
        print(f"⚠ Ya existe un archivo de credenciales en: {credentials_file}")
        response = input("¿Deseas sobrescribirlo? (s/n): ").strip().lower()
        return response == 's'
    
    return True


def main():
    """Main function"""
    if not show_existing_credentials():
        print("Operación cancelada")
        return
    
    try:
        success = setup_aws_credentials()
        
        if success:
            print("✓ Puedes continuar con los siguientes pasos:")
            print("  1. python scripts/test_aws_connection.py")
            print("  2. python scripts/setup_aws_buckets.py")
        
    except KeyboardInterrupt:
        print("\n\nOperación cancelada por el usuario")
    except Exception as e:
        print(f"\n✗ Error: {e}")


if __name__ == "__main__":
    main()

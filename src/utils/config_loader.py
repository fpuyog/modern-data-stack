"""
Configuration Loader Utility
Load YAML configuration files and manage environment variables
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class ConfigLoader:
    """Load and manage configuration from YAML files"""
    
    def __init__(self, config_dir: str = "config"):
        """
        Initialize configuration loader
        
        Args:
            config_dir: Directory containing configuration files
        """
        self.config_dir = Path(config_dir)
        self._configs = {}
    
    def load_config(self, config_file: str) -> Dict[str, Any]:
        """
        Load configuration from YAML file
        
        Args:
            config_file: Name of the configuration file (with or without .yaml extension)
        
        Returns:
            Dictionary with configuration values
        """
        # Add .yaml extension if not present
        if not config_file.endswith('.yaml'):
            config_file = f"{config_file}.yaml"
        
        config_path = self.config_dir / config_file
        
        # Check if config already loaded
        if config_file in self._configs:
            logger.debug(f"Returning cached config: {config_file}")
            return self._configs[config_file]
        
        # Load from file
        if not config_path.exists():
            logger.error(f"Configuration file not found: {config_path}")
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            logger.info(f"Loaded configuration from: {config_path}")
            self._configs[config_file] = config
            return config
        
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file {config_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error loading config {config_path}: {e}")
            raise
    
    def get_aws_config(self) -> Dict[str, Any]:
        """Load AWS configuration"""
        return self.load_config("aws_config.yaml")
    
    def get_db_config(self) -> Dict[str, Any]:
        """Load database configuration"""
        return self.load_config("database_config.yaml")
    
    @staticmethod
    def get_env_var(var_name: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
        """
        Get environment variable value
        
        Args:
            var_name: Name of environment variable
            default: Default value if not found
            required: If True, raise error if variable not found
        
        Returns:
            Environment variable value or default
        """
        value = os.getenv(var_name, default)
        
        if required and value is None:
            logger.error(f"Required environment variable not set: {var_name}")
            raise ValueError(f"Required environment variable not set: {var_name}")
        
        return value


# Singleton instance
config_loader = ConfigLoader()


# Example usage
if __name__ == "__main__":
    loader = ConfigLoader()
    
    # Try loading example configs
    try:
        aws_config = loader.load_config("aws_config.yaml.example")
        print("AWS Config loaded:", aws_config)
    except Exception as e:
        print(f"Error: {e}")

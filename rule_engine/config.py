"""
Configuration management for the Rule Engine
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field
import json


class RuleEngineConfig(BaseModel):
    """Configuration model for the Rule Engine"""
    
    # Core settings
    contracts_dir: str = Field(default="contracts", description="Directory containing contract JSON files")
    log_level: str = Field(default="INFO", description="Logging level")
    log_file: str = Field(default="rule_engine.log", description="Log file path")
    log_rotation: str = Field(default="10 MB", description="Log rotation size")
    log_retention: str = Field(default="30 days", description="Log retention period")
    
    # Processing settings
    max_contracts_per_coupon: int = Field(default=100, description="Maximum contracts to process per coupon")
    processing_timeout: int = Field(default=300, description="Processing timeout in seconds")
    enable_caching: bool = Field(default=True, description="Enable contract caching")
    cache_ttl: int = Field(default=3600, description="Cache TTL in seconds")
    
    # Validation settings
    strict_validation: bool = Field(default=True, description="Enable strict validation")
    validate_contracts_on_load: bool = Field(default=True, description="Validate contracts on load")
    allow_invalid_contracts: bool = Field(default=False, description="Allow processing with invalid contracts")
    
    # Output settings
    output_precision: int = Field(default=2, description="Decimal precision for output values")
    include_metadata: bool = Field(default=True, description="Include metadata in output")
    include_debug_info: bool = Field(default=False, description="Include debug information in output")
    
    # Performance settings
    parallel_processing: bool = Field(default=False, description="Enable parallel processing")
    max_workers: int = Field(default=4, description="Maximum number of worker processes")
    memory_limit: int = Field(default=1024, description="Memory limit in MB")
    
    # API settings (for future use)
    api_host: str = Field(default="localhost", description="API host")
    api_port: int = Field(default=8000, description="API port")
    api_debug: bool = Field(default=False, description="API debug mode")
    
    class Config:
        env_prefix = "RULE_ENGINE_"
        case_sensitive = False


class ConfigManager:
    """Configuration manager for the Rule Engine"""
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize configuration manager
        
        Args:
            config_file: Path to configuration file
        """
        self.config_file = config_file or "rule_engine_config.json"
        self._config = None
        self._load_config()
    
    def _load_config(self) -> None:
        """Load configuration from file or environment"""
        try:
            # Try to load from file first
            if Path(self.config_file).exists():
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                self._config = RuleEngineConfig(**config_data)
            else:
                # Load from environment variables
                self._config = RuleEngineConfig()
                
        except Exception as e:
            print(f"Warning: Failed to load configuration: {e}")
            # Fallback to default configuration
            self._config = RuleEngineConfig()
    
    def get_config(self) -> RuleEngineConfig:
        """Get current configuration"""
        return self._config
    
    def update_config(self, **kwargs) -> None:
        """Update configuration with new values"""
        if self._config:
            for key, value in kwargs.items():
                if hasattr(self._config, key):
                    setattr(self._config, key, value)
    
    def save_config(self) -> None:
        """Save current configuration to file"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self._config.dict(), f, indent=2)
        except Exception as e:
            print(f"Warning: Failed to save configuration: {e}")
    
    def reset_to_defaults(self) -> None:
        """Reset configuration to defaults"""
        self._config = RuleEngineConfig()
    
    def validate_config(self) -> Dict[str, Any]:
        """Validate current configuration"""
        validation_results = {
            'valid': True,
            'warnings': [],
            'errors': []
        }
        
        try:
            # Check contracts directory
            contracts_dir = Path(self._config.contracts_dir)
            if not contracts_dir.exists():
                validation_results['warnings'].append(f"Contracts directory does not exist: {contracts_dir}")
            
            # Check log level
            valid_log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
            if self._config.log_level.upper() not in valid_log_levels:
                validation_results['errors'].append(f"Invalid log level: {self._config.log_level}")
                validation_results['valid'] = False
            
            # Check numeric values
            if self._config.max_contracts_per_coupon <= 0:
                validation_results['errors'].append("max_contracts_per_coupon must be positive")
                validation_results['valid'] = False
            
            if self._config.processing_timeout <= 0:
                validation_results['errors'].append("processing_timeout must be positive")
                validation_results['valid'] = False
            
            if self._config.max_workers <= 0:
                validation_results['errors'].append("max_workers must be positive")
                validation_results['valid'] = False
            
            if self._config.output_precision < 0:
                validation_results['errors'].append("output_precision must be non-negative")
                validation_results['valid'] = False
            
            # Check API settings
            if not (1 <= self._config.api_port <= 65535):
                validation_results['errors'].append("api_port must be between 1 and 65535")
                validation_results['valid'] = False
            
        except Exception as e:
            validation_results['errors'].append(f"Configuration validation error: {str(e)}")
            validation_results['valid'] = False
        
        return validation_results
    
    def get_environment_config(self) -> Dict[str, str]:
        """Get configuration from environment variables"""
        env_config = {}
        
        for field_name, field_info in RuleEngineConfig.__fields__.items():
            env_var_name = f"RULE_ENGINE_{field_name.upper()}"
            env_value = os.getenv(env_var_name)
            
            if env_value is not None:
                env_config[field_name] = env_value
        
        return env_config
    
    def create_sample_config(self, output_file: str = "rule_engine_config_sample.json") -> None:
        """Create a sample configuration file"""
        sample_config = RuleEngineConfig()
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(sample_config.dict(), f, indent=2)
        
        print(f"Sample configuration created: {output_file}")


# Global configuration instance
config_manager = ConfigManager()


def get_config() -> RuleEngineConfig:
    """Get the global configuration instance"""
    return config_manager.get_config()


def update_config(**kwargs) -> None:
    """Update the global configuration"""
    config_manager.update_config(**kwargs)


def save_config() -> None:
    """Save the global configuration"""
    config_manager.save_config()

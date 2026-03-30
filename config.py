"""
config.py — Carrega e valida variáveis de ambiente usando pydantic-settings.
Todas as configurações do sistema são centralizadas aqui.
"""

import json
from typing import Any

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # APIs externas
    datajud_api_key: str
    anthropic_api_key: str
    escavador_api_key: str = ""  # Opcional — fallback CAPTCHA

    # Banco de dados
    db_host: str = "localhost"
    db_port: int = 3306
    db_name: str = "process_monitor"
    db_user: str
    db_password: str

    # Lista de CNJs a monitorar (separados por vírgula no .env)
    cnjs: str

    # Partes do sistema interno em JSON serializado
    partes_sistema: str

    @field_validator("cnjs")
    @classmethod
    def parse_cnjs(cls, v: str) -> str:
        """Garante que pelo menos um CNJ foi informado."""
        items = [c.strip() for c in v.split(",") if c.strip()]
        if not items:
            raise ValueError("CNJS não pode ser vazio")
        return v

    @field_validator("partes_sistema")
    @classmethod
    def parse_partes_sistema(cls, v: str) -> str:
        """Valida que PARTES_SISTEMA é um JSON de lista válido."""
        try:
            parsed = json.loads(v)
            if not isinstance(parsed, list):
                raise ValueError("PARTES_SISTEMA deve ser uma lista JSON")
        except json.JSONDecodeError as exc:
            raise ValueError(f"PARTES_SISTEMA não é JSON válido: {exc}") from exc
        return v

    def get_cnjs_list(self) -> list[str]:
        """Retorna a lista de CNJs como lista de strings."""
        return [c.strip() for c in self.cnjs.split(",") if c.strip()]

    def get_partes_sistema_list(self) -> list[dict[str, Any]]:
        """Retorna as partes do sistema como lista de dicionários."""
        return json.loads(self.partes_sistema)

    def get_db_config(self) -> dict[str, Any]:
        """Retorna configuração de conexão MySQL."""
        return {
            "host": self.db_host,
            "port": self.db_port,
            "database": self.db_name,
            "user": self.db_user,
            "password": self.db_password,
            "charset": "utf8mb4",
            "collation": "utf8mb4_unicode_ci",
        }


# Instância global de configuração — importar em qualquer módulo que precisar
settings = Settings()
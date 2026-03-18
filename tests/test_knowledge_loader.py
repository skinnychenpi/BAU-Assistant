"""
Tests for knowledge/loader.py — YAML config loading.
"""

import pytest
import yaml

from bau.knowledge.loader import get_dag_info, get_task_info, load_dag_config


@pytest.fixture
def config_file(tmp_path):
    """Create a temporary YAML config file."""
    config = {
        "test_dag_a": {
            "confluence_url": "https://confluence.example.com/pages/111",
            "tasks": {
                "collect": {"type": "spark", "yarn_app_name": "app_collect"},
                "parse": {"type": "bash"},
            },
        },
        "test_dag_b": {
            "confluence_url": "https://confluence.example.com/pages/222",
        },
    }
    path = tmp_path / "dag_config.yaml"
    with open(path, "w") as f:
        yaml.dump(config, f)
    return str(path)


def test_load_dag_config(config_file):
    # Clear cache from previous tests
    load_dag_config.cache_clear()
    config = load_dag_config(config_file)
    assert "test_dag_a" in config
    assert "test_dag_b" in config


def test_load_missing_file():
    load_dag_config.cache_clear()
    config = load_dag_config("/nonexistent/path.yaml")
    assert config == {}


def test_get_dag_info(sample_dag_config):
    info = get_dag_info("data_pltingestion_email_demo_linear_dag", sample_dag_config)
    assert "confluence_url" in info
    assert "tasks" in info


def test_get_dag_info_missing():
    info = get_dag_info("nonexistent_dag", {})
    assert info == {}


def test_get_task_info_spark(sample_dag_config):
    info = get_task_info(
        "data_pltingestion_email_demo_linear_dag", "collect", sample_dag_config
    )
    assert info["type"] == "spark"
    assert info["yarn_app_name"] == "pltingestion_email_collect"


def test_get_task_info_bash(sample_dag_config):
    info = get_task_info(
        "data_pltingestion_email_demo_linear_dag", "parse", sample_dag_config
    )
    assert info["type"] == "bash"


def test_get_task_info_missing(sample_dag_config):
    info = get_task_info(
        "data_pltingestion_email_demo_linear_dag", "nonexistent", sample_dag_config
    )
    assert info == {}

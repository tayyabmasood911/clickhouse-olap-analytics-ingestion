import os
import unittest
import yaml

class TestClickHouseOlapConfig(unittest.TestCase):
    def test_core_files_exist(self):
        required_files = [
            "README.md",
            "Makefile",
            "SECURITY.md",
            ".gitattributes",
            ".github/workflows/ci.yml",
            "scripts/healthcheck.sh",
            "scripts/load_test_ingestion.py",
            "config/production.env.example",
            "schema.sql",
            "kafka_engine.sql",
            "docker-compose.yml",
            "docs/architecture.svg"
        ]
        for file_path in required_files:
            self.assertTrue(os.path.exists(file_path), f"Missing required file: {file_path}")

    def test_schema_ddl_integrity(self):
        with open("schema.sql", "r", encoding="utf-8") as f:
            sql = f.read()
        self.assertIn("MergeTree()", sql)
        self.assertIn("SummingMergeTree()", sql)
        self.assertIn("MATERIALIZED VIEW", sql)
        self.assertIn("PARTITION BY", sql)
        self.assertIn("ORDER BY", sql)

    def test_kafka_engine_ddl_integrity(self):
        with open("kafka_engine.sql", "r", encoding="utf-8") as f:
            sql = f.read()
        self.assertIn("ENGINE = Kafka", sql)
        self.assertIn("kafka_broker_list", sql)
        self.assertIn("JSONEachRow", sql)
        self.assertIn("MATERIALIZED VIEW", sql)

    def test_docker_compose_validity(self):
        with open("docker-compose.yml", "r", encoding="utf-8") as f:
            compose = yaml.safe_load(f)
        self.assertIn("services", compose)
        self.assertIn("clickhouse", compose["services"])
        self.assertIn("kafka", compose["services"])

    def test_architecture_diagram_white_theme(self):
        with open("docs/architecture.svg", "r", encoding="utf-8") as f:
            svg = f.read()
        self.assertIn('fill="#ffffff"', svg, "Must have pure white background")
        self.assertIn("ClickHouse", svg)

if __name__ == "__main__":
    unittest.main()

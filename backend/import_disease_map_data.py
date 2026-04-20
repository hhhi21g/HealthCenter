import argparse
import json
import os
from pathlib import Path

import pymysql


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DEFAULT_DATA_PATH = PROJECT_ROOT / "frontend" / "static" / "data" / "disease_map.json"


def get_db_connection():
    connection_kwargs = {
        "user": os.getenv("DB_USER", "healthapp"),
        "password": os.getenv("DB_PASSWORD", ""),
        "database": os.getenv("DB_NAME", "health"),
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
        "autocommit": False,
    }

    unix_socket = os.getenv("DB_UNIX_SOCKET")
    if unix_socket:
        connection_kwargs["unix_socket"] = unix_socket
    else:
        connection_kwargs["host"] = os.getenv("DB_HOST", "127.0.0.1")
        connection_kwargs["port"] = int(os.getenv("DB_PORT", "3306"))

    return pymysql.connect(**connection_kwargs)


def json_text(value):
    return json.dumps(value or [], ensure_ascii=False)


def ensure_schema(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS disease_map_categories (
            id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            sort_order INT UNSIGNED NOT NULL DEFAULT 0,
            UNIQUE KEY uniq_disease_map_category_name (name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS disease_map_seasons (
            season_key VARCHAR(30) NOT NULL PRIMARY KEY,
            label VARCHAR(50) NOT NULL,
            themes_json TEXT NOT NULL,
            sort_order INT UNSIGNED NOT NULL DEFAULT 0
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS disease_map_diseases (
            id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            category VARCHAR(100) NOT NULL,
            summary TEXT NOT NULL,
            actions_json TEXT NOT NULL,
            links_json TEXT NOT NULL,
            sort_order INT UNSIGNED NOT NULL DEFAULT 0,
            UNIQUE KEY uniq_disease_map_disease_name (name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS disease_map_province_profiles (
            province_name VARCHAR(100) NOT NULL PRIMARY KEY,
            region VARCHAR(50) NOT NULL,
            themes_json TEXT NOT NULL,
            actions_json TEXT NOT NULL,
            recommended_json TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            sort_order INT UNSIGNED NOT NULL DEFAULT 0
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    cursor.execute("SHOW COLUMNS FROM disease_map_province_profiles LIKE 'evidence_json'")
    if not cursor.fetchone():
        cursor.execute(
            """
            ALTER TABLE disease_map_province_profiles
            ADD COLUMN evidence_json TEXT NOT NULL
            """
        )


def clear_tables(cursor):
    for table in (
        "disease_map_categories",
        "disease_map_seasons",
        "disease_map_diseases",
        "disease_map_province_profiles",
    ):
        cursor.execute(f"DELETE FROM {table}")


def import_disease_map(data_path):
    with open(data_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            ensure_schema(cursor)
            clear_tables(cursor)

            cursor.executemany(
                """
                INSERT INTO disease_map_categories (name, sort_order)
                VALUES (%s, %s)
                """,
                [
                    (category, index)
                    for index, category in enumerate(data.get("categories", []), start=1)
                ],
            )

            cursor.executemany(
                """
                INSERT INTO disease_map_seasons (season_key, label, themes_json, sort_order)
                VALUES (%s, %s, %s, %s)
                """,
                [
                    (key, value.get("label", ""), json_text(value.get("themes")), index)
                    for index, (key, value) in enumerate(data.get("seasons", {}).items(), start=1)
                ],
            )

            cursor.executemany(
                """
                INSERT INTO disease_map_diseases
                    (name, category, summary, actions_json, links_json, sort_order)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        disease.get("name", ""),
                        disease.get("category", ""),
                        disease.get("summary", ""),
                        json_text(disease.get("actions")),
                        json_text(disease.get("links")),
                        index,
                    )
                    for index, disease in enumerate(data.get("diseases", []), start=1)
                ],
            )

            cursor.executemany(
                """
                INSERT INTO disease_map_province_profiles
                    (province_name, region, themes_json, actions_json, recommended_json, evidence_json, sort_order)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        province_name,
                        profile.get("region", ""),
                        json_text(profile.get("themes")),
                        json_text(profile.get("actions")),
                        json_text(profile.get("recommended")),
                        json_text(profile.get("evidence")),
                        index,
                    )
                    for index, (province_name, profile) in enumerate(
                        data.get("province_profiles", {}).items(),
                        start=1,
                    )
                ],
            )

        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    return {
        "categories": len(data.get("categories", [])),
        "seasons": len(data.get("seasons", {})),
        "diseases": len(data.get("diseases", [])),
        "province_profiles": len(data.get("province_profiles", {})),
    }


def main():
    parser = argparse.ArgumentParser(description="Import disease map JSON data into MySQL tables.")
    parser.add_argument(
        "--json",
        default=str(DEFAULT_DATA_PATH),
        help="Path to disease_map.json. Defaults to frontend/static/data/disease_map.json.",
    )
    args = parser.parse_args()

    counts = import_disease_map(args.json)
    for key, value in counts.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()

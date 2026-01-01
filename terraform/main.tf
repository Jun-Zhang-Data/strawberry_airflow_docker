provider "snowflake" {
  organization_name = var.sf_organization_name
  account_name      = var.sf_account_name
  user              = var.sf_user
  password          = var.sf_password
  role              = "ACCOUNTADMIN"
}

# --- Core objects (already in your state) ---
resource "snowflake_database" "db" {
  name = var.db_name
}

resource "snowflake_warehouse" "wh" {
  name           = var.wh_name
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.db.name
  name     = "RAW"
}

resource "snowflake_schema" "bronze" {
  database = snowflake_database.db.name
  name     = "BRONZE"
}

resource "snowflake_schema" "silver" {
  database = snowflake_database.db.name
  name     = "SILVER"
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.db.name
  name     = "GOLD"
}

# --- Roles (NEW) ---
resource "snowflake_account_role" "role_airflow" {
  name = "ROLE_AIRFLOW"
}

resource "snowflake_account_role" "role_dbt" {
  name = "ROLE_DBT"
}

# Grant roles to your user (so you can test)
resource "snowflake_grant_account_role" "grant_airflow_to_user" {
  role_name = snowflake_account_role.role_airflow.name
  user_name = var.sf_grantee_user
}

resource "snowflake_grant_account_role" "grant_dbt_to_user" {
  role_name = snowflake_account_role.role_dbt.name
  user_name = var.sf_grantee_user
}

# --- Grants: ROLE_AIRFLOW (write RAW only) ---
resource "snowflake_grant_privileges_to_account_role" "airflow_wh_usage" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.role_airflow.name

  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "airflow_db_usage" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.role_airflow.name

  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.db.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "airflow_raw_schema_usage_create" {
  privileges        = ["USAGE", "CREATE TABLE"]
  account_role_name = snowflake_account_role.role_airflow.name

  on_schema {
    schema_name = "\"${snowflake_database.db.name}\".\"${snowflake_schema.raw.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "airflow_raw_tables_all" {
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE"]
  account_role_name = snowflake_account_role.role_airflow.name

  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.db.name}\".\"${snowflake_schema.raw.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "airflow_raw_tables_future" {
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE"]
  account_role_name = snowflake_account_role.role_airflow.name

  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.db.name}\".\"${snowflake_schema.raw.name}\""
    }
  }
}

# --- Grants: ROLE_DBT (read RAW + build BRONZE/SILVER/GOLD) ---
resource "snowflake_grant_privileges_to_account_role" "dbt_wh_usage" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.role_dbt.name

  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_db_usage" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.role_dbt.name

  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.db.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_raw_tables_all" {
  privileges        = ["SELECT"]
  account_role_name = snowflake_account_role.role_dbt.name

  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.db.name}\".\"${snowflake_schema.raw.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_raw_tables_future" {
  privileges        = ["SELECT"]
  account_role_name = snowflake_account_role.role_dbt.name

  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.db.name}\".\"${snowflake_schema.raw.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_bronze_create" {
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  account_role_name = snowflake_account_role.role_dbt.name

  on_schema {
    schema_name = "\"${snowflake_database.db.name}\".\"${snowflake_schema.bronze.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_silver_create" {
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  account_role_name = snowflake_account_role.role_dbt.name

  on_schema {
    schema_name = "\"${snowflake_database.db.name}\".\"${snowflake_schema.silver.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_gold_create" {
  privileges        = ["USAGE", "CREATE VIEW"]
  account_role_name = snowflake_account_role.role_dbt.name

  on_schema {
    schema_name = "\"${snowflake_database.db.name}\".\"${snowflake_schema.gold.name}\""
  }
}

resource "google_datastream_stream" "default" {
  project       = var.project_id_stream
  stream_id     = var.datastream-id
  desired_state = "RUNNING"
  location      = var.region
  display_name  = var.datastream-display-name
  source_config {
    source_connection_profile = "projects/${var.project_id_stream}/locations/${var.region}/connectionProfiles/${var.source-connection-profile-id}"
    mysql_source_config {
      include_objects {
        mysql_databases {
          database = var.includedatabase
          dynamic "mysql_tables" {
            for_each = var.includetables
            content {
              table = mysql_tables.value
            }
          }
        }
      }
      exclude_objects {
        mysql_databases {
          database = var.exludedatabase
          dynamic "mysql_tables" {
            for_each = var.excludetables
            content {
              table = mysql_tables.value
            }
          }
        }
      }
    }
  }
  destination_config {
    destination_connection_profile = "projects/${var.project_id_stream}/locations/${var.region}/connectionProfiles/${var.destination-connection-profile-id}"
    gcs_destination_config {
      json_file_format {
        schema_file_format = "NO_SCHEMA_FILE"
      }
    }
  }

  backfill_all {
  }

}
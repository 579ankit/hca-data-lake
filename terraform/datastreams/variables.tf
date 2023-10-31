variable "project_id_stream" {
  description = "Project being used for datastream related resources"
  type        = string
}

variable "region" {
  type = string
}

variable "location" {
  type = string
}

variable "includedatabase" {
  type = string
}

variable "includetables" {
  type = list(string)

}

variable "exludedatabase" {
  type = string
}

variable "excludetables" {
  type = list(string)
}

variable "source-connection-display-name" {
  type = string
}

variable "destination-connection-display-name" {
  type = string
}

variable "source-connection-profile-id" {
  type = string
}

variable "destination-connection-profile-id" {
  type = string
}

variable "datastream-id" {
  type = string
}

variable "datastream-display-name" {
  type = string
}
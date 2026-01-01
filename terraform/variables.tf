variable "sf_organization_name" {
  type = string
}

variable "sf_account_name" {
  type = string
}

variable "sf_user" {
  type = string
}

variable "sf_password" {
  type      = string
  sensitive = true
}

variable "db_name" {
  type    = string
  default = "STRAWBERRY"
}

variable "wh_name" {
  type    = string
  default = "WH_STRAWBERRY"
}

variable "sf_grantee_user" {
  type = string
}

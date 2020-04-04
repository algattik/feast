variable "appname" {
  type = string
  description = "Application name. Use only lowercase letters and numbers"
  default = "beamhdipoc"
}

variable "environment" {
  type    = string
  description = "Environment name, e.g. 'dev' or 'cd'"
  default = "dev"
}

variable "location" {
  type    = string
  description = "Azure region where to create resources."
  default = "East US"
}

variable "hdinsight_user" {
  type    = string
  description = "HDInsight username"
  default = "admin"
}

variable "ssh_user" {
  type    = string
  description = "SSH username"
  default = "sshuser"
}

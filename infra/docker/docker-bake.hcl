variable "DOCKER_REGISTRY" {
  default = "304971447450.dkr.ecr.eu-west-3.amazonaws.com"
}

variable "AWS_REGION" {
  default = "eu-west-3"
}

variable "TYPE" {
  default = "infra"
}

variable "DOCKERFILE_DIR" {
  default = "docker-build/"
}

variable "KAFKA_CONNECT_VERSION" {
  default = "7.9.3"
}

variable "CI_COMMIT_SHORT_SHA" {
  default = "localcommittest"
}

variable "CI_COMMIT_REF_NAME" {
  default = "localrefbranchtag"
}

variable "KAFKA_CONNECT_DOCKER_TAG" {
  default = "${KAFKA_CONNECT_VERSION}-${CI_COMMIT_SHORT_SHA}"
}

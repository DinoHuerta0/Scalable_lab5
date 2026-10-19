data "aws_vpc" "default" { default = true }

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "availability-zone"
    values = ["${var.aws_region}a", "${var.aws_region}b"]
  }
}

# Tu IP pública, para abrir el Browser de Neo4j, Grafana y SSH solo a ti
data "http" "my_ip" {
  url = "https://checkip.amazonaws.com"
}

locals {
  # Si defines allowed_ssh_cidr se usa ese valor; si no, tu IP detectada /32
  admin_cidr = coalesce(var.allowed_ssh_cidr, "${chomp(data.http.my_ip.response_body)}/32")
}

resource "aws_security_group" "lambda" {
  name   = "${var.project_name}-lambda-sg"
  vpc_id = data.aws_vpc.default.id
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "neo4j" {
  name   = "${var.project_name}-neo4j-sg"
  vpc_id = data.aws_vpc.default.id

  # La Lambda escribe por la API HTTP de Neo4j (red privada de la VPC)
  ingress {
    description     = "Neo4j HTTP desde la Lambda"
    from_port       = 7474
    to_port         = 7474
    protocol        = "tcp"
    security_groups = [aws_security_group.lambda.id]
  }

  # Tú: Browser de Neo4j (7474), Bolt para el Browser (7687), Grafana (3000) y SSH
  dynamic "ingress" {
    for_each = [7474, 7687, 3000, 22]
    content {
      from_port   = ingress.value
      to_port     = ingress.value
      protocol    = "tcp"
      cidr_blocks = [local.admin_cidr]
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

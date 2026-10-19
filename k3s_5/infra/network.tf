data "aws_vpc" "default" { default = true }

# --- AZs donde el tipo de instancia elegido sí está disponible ---
data "aws_ec2_instance_type_offerings" "supported" {
  location_type = "availability-zone"

  filter {
    name   = "instance-type"
    values = [var.instance_type]
  }
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "availability-zone"
    values = data.aws_ec2_instance_type_offerings.supported.locations
  }
}

# Tu IP pública, para abrir la API, Neo4j, Grafana, Prometheus y SSH solo a ti
data "http" "my_ip" {
  url = "https://checkip.amazonaws.com"
}

locals {
  # Si defines allowed_ssh_cidr se usa ese valor; si no, tu IP detectada /32
  admin_cidr = coalesce(var.allowed_ssh_cidr, "${chomp(data.http.my_ip.response_body)}/32")

  # Puertos del nodo. k3s (ServiceLB) publica cada Service LoadBalancer en el puerto del nodo.
  open_ports = {
    22   = "SSH (y el despliegue que hace Terraform)"
    80   = "API del juez (Kong)"
    3000 = "Grafana"
    7474 = "Neo4j Browser y API HTTP"
    7687 = "Neo4j Bolt (para el Browser)"
    9090 = "Prometheus (lo consulta metrics.py)"
  }
}

resource "aws_security_group" "node" {
  name        = "${var.project_name}-sg"
  description = "Nodos k3s del Lab 5: todo solo desde tu IP"
  vpc_id      = data.aws_vpc.default.id

  dynamic "ingress" {
    for_each = local.open_ports
    content {
      description = ingress.value
      from_port   = ingress.key
      to_port     = ingress.key
      protocol    = "tcp"
      cidr_blocks = [local.admin_cidr]
    }
  }

  # Entre los dos nodos: API de k3s (6443), red de pods de flannel (8472/UDP),
  # kubelet (10250) y el tráfico de los pods. Solo desde instancias de este mismo SG.
  ingress {
    description = "Trafico entre los nodos de k3s"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project_name}-sg" }
}

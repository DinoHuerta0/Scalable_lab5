# --- Key Pair generado por Terraform (se crea y destruye con el resto) ---
# El .pem se guarda en infra/ y está en .gitignore. Terraform también lo usa para
# entrar por SSH y desplegar el pipeline (deploy.tf).
resource "tls_private_key" "ssh" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_key_pair" "ssh" {
  key_name   = "${var.project_name}-key"
  public_key = tls_private_key.ssh.public_key_openssh
}

resource "local_sensitive_file" "pem" {
  content         = tls_private_key.ssh.private_key_pem
  filename        = "${path.module}/${var.project_name}-key.pem"
  file_permission = "0600"
}

# --- Restringir el .pem solo a tu usuario (Git Bash en Windows, o Linux/Mac) ---
# En Windows, file_permission no se traduce a permisos NTFS, por eso se usa icacls.
# MSYS_NO_PATHCONV=1 evita que Git Bash convierta "/grant:r" en una ruta de archivo.
resource "terraform_data" "fix_pem_permissions" {
  triggers_replace = [local_sensitive_file.pem.id]

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = <<-EOT
      KEY="${abspath(local_sensitive_file.pem.filename)}"
      if command -v icacls >/dev/null 2>&1; then
        MSYS_NO_PATHCONV=1 icacls "$KEY" /inheritance:r
        MSYS_NO_PATHCONV=1 icacls "$KEY" /grant:r "$USERNAME:(F)"
      else
        chmod 600 "$KEY"
      fi
    EOT
  }
}

# --- AMI más reciente de Amazon Linux 2023 ---
data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-kernel-6.1-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

locals {
  scripts_dir = "${path.module}/../scripts"
  src_dir     = "${path.module}/../src"
  k8s_dir     = "${path.module}/../k8s"
}

# --- Clúster de k3s con dos nodos ---
# AWS Academy no permite instancias más grandes que large, así que el clúster se
# reparte en dos t3.large (2 vCPU y 8 GB cada una):
#
#   server  k3s server + Knative + Kong + funciones + Prometheus + Grafana
#   data    k3s agent con la etiqueta y el taint lab5/role=data: solo corren ahí los
#           pods que lo toleran (Kafka y Neo4j), así no compiten por CPU ni memoria
#
# user_data solo instala la plataforma. El pipeline lo despliega deploy.tf por SSH,
# así que cambiar una variable del pipeline o el código de src/ NO recrea las instancias.

# Secreto compartido para que el agente se una al servidor
resource "random_password" "k3s_token" {
  length  = 48
  special = false
}

resource "aws_instance" "server" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.instance_type
  subnet_id              = sort(data.aws_subnets.default.ids)[0]
  vpc_security_group_ids = [aws_security_group.node.id]
  key_name               = aws_key_pair.ssh.key_name

  root_block_device {
    volume_size = var.volume_size
    volume_type = "gp3"
  }

  user_data = replace(templatefile("${local.scripts_dir}/user_data.sh.tftpl", {
    knative_version = var.knative_version
    k3s_token       = random_password.k3s_token.result
  }), "\r\n", "\n")
  user_data_replace_on_change = true

  tags = { Name = "${var.project_name}-server" }
}

resource "aws_instance" "data" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.data_instance_type
  subnet_id              = sort(data.aws_subnets.default.ids)[0] # misma subred que el servidor
  vpc_security_group_ids = [aws_security_group.node.id]
  key_name               = aws_key_pair.ssh.key_name

  root_block_device {
    volume_size = var.volume_size
    volume_type = "gp3"
  }

  # El agente reintenta solo hasta que el servidor responde, así que las dos
  # instancias arrancan casi al mismo tiempo
  user_data = replace(templatefile("${local.scripts_dir}/user_data_agent.sh.tftpl", {
    server_ip = aws_instance.server.private_ip
    k3s_token = random_password.k3s_token.result
  }), "\r\n", "\n")
  user_data_replace_on_change = true

  tags = { Name = "${var.project_name}-data" }
}

# --- Al destruir las instancias, borrar sus huellas de ~/.ssh/known_hosts ---
resource "terraform_data" "known_hosts_cleanup" {
  for_each = {
    server = aws_instance.server.public_ip
    data   = aws_instance.data.public_ip
  }
  input = each.value

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["bash", "-c"]
    command     = "ssh-keygen -R ${self.input}"
    on_failure  = continue
  }
}

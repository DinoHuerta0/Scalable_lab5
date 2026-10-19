resource "random_password" "neo4j" {
  length  = 20
  special = false
}

# --- Key Pair generado por Terraform (se crea y destruye con el resto) ---
# El .pem se guarda en infra/ y está en .gitignore
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
}

resource "aws_instance" "neo4j" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.instance_type
  subnet_id              = data.aws_subnets.default.ids[0]
  vpc_security_group_ids = [aws_security_group.neo4j.id]
  key_name               = aws_key_pair.ssh.key_name

  # Rol de Academy: Grafana lo usa para leer CloudWatch
  iam_instance_profile = "LabInstanceProfile"

  # IMDSv2 con 2 saltos para que el contenedor de Grafana alcance las credenciales
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  user_data = templatefile("${local.scripts_dir}/user_data.sh.tftpl", {
    neo4j_password   = random_password.neo4j.result
    neo4j_image      = var.neo4j_image
    grafana_password = random_password.grafana.result
    grafana_image    = var.grafana_image
    neo4j_plugin     = var.grafana_neo4j_plugin
    region           = var.aws_region
    dashboard_b64    = base64gzip(jsonencode(local.grafana_dashboard))
  })
  user_data_replace_on_change = true

  tags = { Name = "${var.project_name}-neo4j-grafana" }
}

# --- Al destruir la instancia, borrar su huella de ~/.ssh/known_hosts ---
resource "terraform_data" "known_hosts_cleanup" {
  input = aws_instance.neo4j.public_ip

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["bash", "-c"]
    command     = "ssh-keygen -R ${self.input}"
    on_failure  = continue
  }
}

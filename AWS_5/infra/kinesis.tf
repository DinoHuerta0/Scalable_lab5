# El "tópico": cada envío calificado por el juez es un registro.
# Partition key = user_id, así los envíos de un mismo usuario conservan su orden.
#
# AWS Academy bloquea (SCP) kinesis:IncreaseStreamRetentionPeriod, y el recurso
# aws_kinesis_stream del provider siempre la llama al crear el stream. Por eso el
# stream se crea con el AWS CLI (solo CreateStream) y se lee con un data source.
#
# Cambiar var.shards borra y vuelve a crear el stream (Academy tampoco garantiza
# UpdateShardCount). Haz `python db.py reset` antes de la siguiente prueba.
locals {
  stream_name = "${var.project_name}-submissions"
  aws_args    = "--stream-name ${local.stream_name} --profile ${var.aws_profile} --region ${var.aws_region}"
}

resource "terraform_data" "stream" {
  triggers_replace = [local.stream_name, var.shards]

  input = {
    aws_args = local.aws_args
  }

  provisioner "local-exec" {
    command = "aws kinesis create-stream ${local.aws_args} --shard-count ${var.shards} && aws kinesis wait stream-exists ${local.aws_args}"
  }

  provisioner "local-exec" {
    when    = destroy
    command = "aws kinesis delete-stream ${self.input.aws_args} --enforce-consumer-deletion && aws kinesis wait stream-not-exists ${self.input.aws_args}"
  }
}

data "aws_kinesis_stream" "submissions" {
  name       = local.stream_name
  depends_on = [terraform_data.stream]
}

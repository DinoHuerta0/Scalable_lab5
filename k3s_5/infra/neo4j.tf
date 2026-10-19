# Neo4j corre como StatefulSet dentro de k3s (k8s/neo4j.yaml), con su disco en el nodo.
# La contraseña la guarda deploy.sh en el Secret neo4j-auth.
resource "random_password" "neo4j" {
  length  = 20
  special = false
}

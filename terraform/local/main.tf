terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0.0"
    }
  }
}

provider "docker" {}

resource "docker_network" "lab_net" {
  name = "lab3-network"
}

resource "docker_image" "app_image" {
  name = "lab3-app:local"
  build {
    context = "../../app"
    tag     = ["lab3-app:latest"]
  }
}

# --- COORDINATOR ---
resource "docker_container" "coordinator" {
  name  = "coordinator"
  image = docker_image.app_image.name
  restart = "always"
  
  env = [
    "PYTHONUNBUFFERED=1",
    "APP_FILE=coordinator.py"
  ]
  
  ports {
    internal = 8000
    external = 8000
  }
  
  networks_advanced {
    name = docker_network.lab_net.name
    aliases = ["coordinator"]
  }
}

# ================= SHARD 1 GROUP =================

# 1. Leader (Унікальний)
resource "docker_container" "s1_leader" {
  name  = "s1-leader"
  image = docker_image.app_image.name
  restart = "always"

  env = [
    "PYTHONUNBUFFERED=1",
    "APP_FILE=shard.py",
    "ROLE=leader",
    "SHARD_ID=shard-1",
    "COORDINATOR_URL=http://coordinator:8000",
    "MY_ADDRESS=http://s1-leader:8000"
  ]

  volumes {
    host_path      = "${abspath(path.cwd)}/data/s1-leader"
    container_path = "/app/data"
  }

  networks_advanced {
    name = docker_network.lab_net.name
    aliases = ["s1-leader"]
  }
  
  depends_on = [docker_container.coordinator]
}

# 2. Followers (Масштабуємо через count)
resource "docker_container" "s1_followers" {
  count = 2  # <--- Створюємо 2 репліки: s1-follower-1, s1-follower-2

  name  = "s1-follower-${count.index + 1}"
  image = docker_image.app_image.name
  restart = "always"

  env = [
    "PYTHONUNBUFFERED=1",
    "APP_FILE=shard.py",
    "ROLE=follower",
    "SHARD_ID=shard-1",
    "COORDINATOR_URL=http://coordinator:8000",
    # Генеруємо унікальну адресу: http://s1-follower-1:8000
    "MY_ADDRESS=http://s1-follower-${count.index + 1}:8000",
    "LEADER_URL=http://s1-leader:8000"
  ]

  volumes {
    # Кожен отримує свою папку: data/s1-follower-1, data/s1-follower-2
    host_path      = "${abspath(path.cwd)}/data/s1-follower-${count.index + 1}"
    container_path = "/app/data"
  }

  networks_advanced {
    name = docker_network.lab_net.name
    aliases = ["s1-follower-${count.index + 1}"]
  }

  depends_on = [docker_container.s1_leader]
}

# ================= SHARD 2 GROUP =================

# 1. Leader
resource "docker_container" "s2_leader" {
  name  = "s2-leader"
  image = docker_image.app_image.name
  restart = "always"

  env = [
    "PYTHONUNBUFFERED=1",
    "APP_FILE=shard.py",
    "ROLE=leader",
    "SHARD_ID=shard-2",
    "COORDINATOR_URL=http://coordinator:8000",
    "MY_ADDRESS=http://s2-leader:8000"
  ]

  volumes {
    host_path      = "${abspath(path.cwd)}/data/s2-leader"
    container_path = "/app/data"
  }

  networks_advanced {
    name = docker_network.lab_net.name
    aliases = ["s2-leader"]
  }

  depends_on = [docker_container.coordinator]
}

# 2. Followers (Масштабуємо через count)
resource "docker_container" "s2_followers" {
  count = 2  # <--- Створюємо 2 репліки: s2-follower-1, s2-follower-2

  name  = "s2-follower-${count.index + 1}"
  image = docker_image.app_image.name
  restart = "always"

  env = [
    "PYTHONUNBUFFERED=1",
    "APP_FILE=shard.py",
    "ROLE=follower",
    "SHARD_ID=shard-2",
    "COORDINATOR_URL=http://coordinator:8000",
    "MY_ADDRESS=http://s2-follower-${count.index + 1}:8000",
    "LEADER_URL=http://s2-leader:8000"
  ]

  volumes {
    host_path      = "${abspath(path.cwd)}/data/s2-follower-${count.index + 1}"
    container_path = "/app/data"
  }

  networks_advanced {
    name = docker_network.lab_net.name
    aliases = ["s2-follower-${count.index + 1}"]
  }

  depends_on = [docker_container.s2_leader]
}


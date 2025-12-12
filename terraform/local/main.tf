terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0.0"
    }
  }
}

provider "docker" {}

# 1. Створюємо мережу, щоб контейнери бачили один одного по імені
resource "docker_network" "lab_net" {
  name = "lab3-network"
}

# 2. Збираємо образ (один на всіх)
resource "docker_image" "app_image" {
  name = "lab3-app:local"
  build {
    context = "../../app" # Шлях до папки з кодом
    tag     = ["lab3-app:latest"]
  }
}

# 3. COORDINATOR
resource "docker_container" "coordinator" {
  name  = "coordinator"
  image = docker_image.app_image.name
  restart = "always"
  
  env = [
    "APP_FILE=coordinator.py",
    # Координатор спочатку пустий, шарди самі до нього постукають
    "SHARD_NODES=" 
  ]
  
  ports {
    internal = 8000
    external = 8000 # Доступний зовні як localhost:8000
  }
  
  networks_advanced {
    name = docker_network.lab_net.name
    aliases = ["coordinator"]
  }
}

# --- SHARD 1 GROUP (Replication Demo) ---

# Shard 1 LEADER
resource "docker_container" "s1_leader" {
  name  = "s1-leader"
  image = docker_image.app_image.name
  restart = "always"

  env = [
    "APP_FILE=shard.py",
    "ROLE=leader",
    "COORDINATOR_URL=http://coordinator:8000",
    "MY_ADDRESS=http://s1-leader:8000"
  ]

  # Монтуємо папку для WAL логів (Durability)
  # Використовуємо абсолютний шлях для локального запуску
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

# Shard 1 FOLLOWER
resource "docker_container" "s1_follower" {
  name  = "s1-follower"
  image = docker_image.app_image.name
  restart = "always"

  env = [
    "APP_FILE=shard.py",
    "ROLE=follower",
    "COORDINATOR_URL=http://coordinator:8000",
    "MY_ADDRESS=http://s1-follower:8000",
    "LEADER_URL=http://s1-leader:8000" # Вказуємо на лідера
  ]

  volumes {
    host_path      = "${abspath(path.cwd)}/data/s1-follower"
    container_path = "/app/data"
  }

  networks_advanced {
    name = docker_network.lab_net.name
    aliases = ["s1-follower"]
  }

  depends_on = [docker_container.s1_leader]
}

# --- SHARD 2 (Sharding Demo) ---

# Shard 2 LEADER (Single node for simplicity)
resource "docker_container" "s2_leader" {
  name  = "s2-leader"
  image = docker_image.app_image.name
  restart = "always"

  env = [
    "APP_FILE=shard.py",
    "ROLE=leader",
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

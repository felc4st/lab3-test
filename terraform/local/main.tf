terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0.0"
    }
  }
}

provider "docker" {}

# 1. Збираємо Docker Image локально
resource "docker_image" "app_image" {
  name = "lab2-app:local"
  build {
    context = "../../app" # Шлях до папки з кодом відносно цього файлу
    tag     = ["lab2-app:latest"]
  }
}

# 2. Створюємо мережу, щоб контейнери бачили один одного
resource "docker_network" "lab_network" {
  name = "lab2-net"
}

# 3. Coordinator Container
resource "docker_container" "coordinator" {
  name  = "coordinator"
  image = docker_image.app_image.name
  restart = "always"

  env = [
    "APP_FILE=coordinator.py",
    "SHARD_NODES=" # Порожній, бо шарди реєструються самі
  ]

  ports {
    internal = 8000
    external = 8000 # Доступ з хоста по localhost:8000
  }

  networks_advanced {
    name = docker_network.lab_network.name
    aliases = ["coordinator"]
  }
}

# 4. Shard 1 Container
resource "docker_container" "shard1" {
  name  = "shard-1"
  image = docker_image.app_image.name
  restart = "always"

  env = [
    "APP_FILE=shard.py",
    "COORDINATOR_URL=http://coordinator:8000",
    "MY_ADDRESS=http://shard-1:8000"
  ]

  ports {
    internal = 8000
    external = 8001
  }


  networks_advanced {
    name = docker_network.lab_network.name
    aliases = ["shard-1"]
  }
  
  # Чекаємо запуску координатора (хоча авто-реєстрація має retry logic)
  depends_on = [docker_container.coordinator]
}

# 5. Shard 2 Container
resource "docker_container" "shard2" {
  name  = "shard-2"
  image = docker_image.app_image.name
  restart = "always"

  env = [
    "APP_FILE=shard.py",
    "COORDINATOR_URL=http://coordinator:8000",
    "MY_ADDRESS=http://shard-2:8000"
  ]

  ports {
    internal = 8000
    external = 8002
  }


  networks_advanced {
    name = docker_network.lab_network.name
    aliases = ["shard-2"]
  }

  depends_on = [docker_container.coordinator]

}

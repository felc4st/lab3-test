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

# --- SHARD 1 GROUP ---

resource "docker_container" "s1_leader" {
  name  = "s1-leader"
  image = docker_image.app_image.name
  restart = "always"

  env = [
    "PYTHONUNBUFFERED=1",
    "APP_FILE=shard.py",
    "ROLE=leader",
    "SHARD_ID=shard-1",    # <--- ВАЖЛИВО!
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

resource "docker_container" "s1_follower" {
  name  = "s1-follower"
  image = docker_image.app_image.name
  restart = "always"

  env = [
    "PYTHONUNBUFFERED=1",
    "APP_FILE=shard.py",
    "ROLE=follower",
    "SHARD_ID=shard-1",    # <--- ВАЖЛИВО!
    "COORDINATOR_URL=http://coordinator:8000",
    "MY_ADDRESS=http://s1-follower:8000",
    "LEADER_URL=http://s1-leader:8000"
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

# --- SHARD 2 ---

resource "docker_container" "s2_leader" {
  name  = "s2-leader"
  image = docker_image.app_image.name
  restart = "always"

  env = [
    "PYTHONUNBUFFERED=1",
    "APP_FILE=shard.py",
    "ROLE=leader",
    "SHARD_ID=shard-2",    # <--- ВАЖЛИВО!
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

/*
 * Surgewave Public Benchmark — AWS reference instance.
 *
 * Spin up a clean c7i.4xlarge in eu-central-1, run `surgewave-bench
 * public`, copy the results back, then `terraform destroy`. The
 * estimated wall-clock is ~2 h per release run; expected cost per
 * run is ~$1.90 on-demand / ~$0.74 spot (see COSTS.md for the
 * breakdown). Mandatory `terraform destroy` at the end is the
 * defence against the only relevant fail-mode: leaving the instance
 * running. A forgotten c7i.4xlarge for a month is $640.
 *
 * Usage:
 *   terraform init
 *   terraform apply -var='key_name=surgewave-bench'
 *   # → outputs ssh_command + result_pull_command
 *   <run benchmark via SSH per COSTS.md>
 *   terraform destroy   # MANDATORY
 */

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }
}

provider "aws" {
  region = var.region
}

variable "region" {
  description = "AWS region. eu-central-1 (Frankfurt) by default for low Kuestenlogik-Setup-latency."
  type        = string
  default     = "eu-central-1"
}

variable "key_name" {
  description = "EC2 keypair name (already created in the target region). Generate one with `aws ec2 create-key-pair --key-name surgewave-bench --query KeyMaterial --output text > ~/.ssh/surgewave-bench.pem && chmod 600 ~/.ssh/surgewave-bench.pem`."
  type        = string
}

variable "instance_type" {
  description = "Reference is c7i.4xlarge (16 vCPU, 32 GiB, AVX-512). c6a.4xlarge ~30 % cheaper, AMD EPYC, no AVX-512 — dev-iteration only, not for marketing numbers."
  type        = string
  default     = "c7i.4xlarge"
}

variable "use_spot" {
  description = "Use Spot pricing (~60 % cheaper, can be interrupted). Default off because a Public-Run that gets killed mid-way wastes the wall-clock + has to be re-run anyway."
  type        = bool
  default     = false
}

# Canonical's latest Ubuntu 24.04 LTS amd64 AMI in the chosen region.
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]  # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
}

resource "aws_security_group" "bench" {
  name        = "surgewave-bench-sg"
  description = "Surgewave Public Benchmark — SSH inbound from anywhere (tighten in production)."

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Purpose = "surgewave-bench"
  }
}

resource "aws_instance" "bench" {
  ami           = data.aws_ami.ubuntu.id
  instance_type = var.instance_type
  key_name      = var.key_name

  vpc_security_group_ids = [aws_security_group.bench.id]

  root_block_device {
    volume_type = "gp3"
    volume_size = 200
    iops        = 10000
    throughput  = 250
    # Delete on terminate so `terraform destroy` doesn't leave an
    # orphaned $20/month volume behind. The whole point of this
    # instance is to be ephemeral.
    delete_on_termination = true
  }

  dynamic "instance_market_options" {
    for_each = var.use_spot ? [1] : []
    content {
      market_type = "spot"
      spot_options {
        spot_instance_type = "one-time"
      }
    }
  }

  # Cloud-init that installs Docker + .NET 10 + the surgewave-bench
  # tool. The user just needs to SSH in and run `surgewave-bench
  # public --output …` once.
  user_data = <<-EOT
              #!/bin/bash
              set -eux
              apt-get update
              apt-get install -y docker.io
              curl -fsSL https://dot.net/v1/dotnet-install.sh | bash -s -- --version 10.0.7 --install-dir /usr/share/dotnet
              ln -s /usr/share/dotnet/dotnet /usr/local/bin/dotnet
              sudo -u ubuntu /usr/local/bin/dotnet tool install -g Kuestenlogik.Surgewave.Benchmarks
              EOT

  tags = {
    Name    = "surgewave-bench"
    Purpose = "surgewave-bench"
  }
}

output "ssh_command" {
  description = "SSH into the instance to launch the benchmark run."
  value       = "ssh -i ~/.ssh/${var.key_name}.pem ubuntu@${aws_instance.bench.public_ip}"
}

output "run_command" {
  description = "Once SSH'd in, run this to start the public benchmark."
  value       = "~/.dotnet/tools/surgewave-bench public --message-count 1000000 --payload 1024 --output ~/results.md --json ~/results.json"
}

output "result_pull_command" {
  description = "From your local machine, copy the results back."
  value       = "scp -i ~/.ssh/${var.key_name}.pem 'ubuntu@${aws_instance.bench.public_ip}:~/results.*' docs/benchmarks/"
}

output "destroy_reminder" {
  description = "MANDATORY cleanup — a forgotten c7i.4xlarge is $640/month."
  value       = "Run `terraform destroy` when done. Do not skip this step."
}

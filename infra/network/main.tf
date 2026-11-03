# ==============================================================================
# Virtual Private Cloud (VPC)
# ==============================================================================
resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "capstone-${var.environment}-vpc"
    Tier = "networking-core"
  }
}

# ==============================================================================
# Internet Gateway (Public Egress/Ingress)
# ==============================================================================
resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "capstone-${var.environment}-igw"
    Tier = "public"
  }
}

# ==============================================================================
# Public Subnets (Multi-AZ)
# ==============================================================================
resource "aws_subnet" "public" {
  count                   = length(var.public_subnet_cidrs)
  vpc_id                  = aws_vpc.main.id
  cidr_block              = var.public_subnet_cidrs[count.index]
  availability_zone       = var.availability_zones[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name             = "capstone-${var.environment}-public-${var.availability_zones[count.index]}"
    Tier             = "public"
    AvailabilityZone = var.availability_zones[count.index]
  }
}

# ==============================================================================
# Private Subnets (Multi-AZ)
# ==============================================================================
resource "aws_subnet" "private" {
  count                   = length(var.private_subnet_cidrs)
  vpc_id                  = aws_vpc.main.id
  cidr_block              = var.private_subnet_cidrs[count.index]
  availability_zone       = var.availability_zones[count.index]
  map_public_ip_on_launch = false

  tags = {
    Name             = "capstone-${var.environment}-private-${var.availability_zones[count.index]}"
    Tier             = "private"
    AvailabilityZone = var.availability_zones[count.index]
  }
}

# ==============================================================================
# Public Routing (Connected to Internet Gateway)
# ==============================================================================
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }

  tags = {
    Name = "capstone-${var.environment}-public-rt"
    Tier = "public"
  }
}

resource "aws_route_table_association" "public" {
  count          = length(aws_subnet.public)
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# ==============================================================================
# Private Routing (Strictly Isolated - No Direct IGW Route)
# ==============================================================================
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  # Deliberately omits 0.0.0.0/0 IGW route to maintain private subnet isolation
  tags = {
    Name = "capstone-${var.environment}-private-rt"
    Tier = "private"
  }
}

resource "aws_route_table_association" "private" {
  count          = length(aws_subnet.private)
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}


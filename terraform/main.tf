# For suggested naming conventions, refer to:
#   https://docs.microsoft.com/en-us/azure/cloud-adoption-framework/ready/azure-best-practices/naming-and-tagging

resource "azurerm_resource_group" "main" {
  name     = "rg-${var.appname}-${var.environment}"
  location = var.location
}

# Generate strong password

resource "random_password" "vm_password" {
  length = 24
  special = true
  override_special = "!@#$%&*()-_=+[]:?"
  min_upper = 1
  min_lower = 1
  min_numeric = 1
  min_special = 1
}

locals {
  vm_password = random_password.vm_password.result
  hdinsight_password = random_password.vm_password.result
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  ssh_pub_key         = file("~/.ssh/id_rsa.pub")
  vpn_cert_data       = file("vpn-certs/caCert.txt")
}

# Create virtual network

resource "azurerm_virtual_network" "main" {
  name                = "vnet-${var.appname}-${var.environment}"
  address_space       = ["10.100.0.0/16"]
  resource_group_name = local.resource_group_name
  location            = local.location
}

# Create subnets

resource "azurerm_subnet" "gateway" {
  name                 = "GatewaySubnet"
  resource_group_name = local.resource_group_name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefix       = "10.100.1.0/27"
}

resource "azurerm_subnet" "jumphost" {
  name                 = "JumphostSubnet"
  resource_group_name = local.resource_group_name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefix       = "10.100.2.0/27"
}


resource "azurerm_subnet" "containers" {
  name                 = "ContainersSubnet"
  resource_group_name = local.resource_group_name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefix       = "10.100.3.0/24"

  delegation {
    name = "delegation"

    service_delegation {
      name    = "Microsoft.ContainerInstance/containerGroups"
      actions = [ "Microsoft.Network/virtualNetworks/subnets/action" ]
    }
  }
}

resource "azurerm_subnet" "hdinsight" {
  name                 = "HDInsightSubnet"
  resource_group_name = local.resource_group_name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefix       = "10.100.4.0/24"
}

# Create HDInsight storage

resource "azurerm_storage_account" "hdinsight" {
  name                = "hdi${var.appname}${var.environment}"
  resource_group_name = local.resource_group_name
  location            = local.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

# Create HDInsight Spark

resource "azurerm_storage_container" "spark" {
  name                  = "spark"
  storage_account_name  = azurerm_storage_account.hdinsight.name
  container_access_type = "private"
}

resource "azurerm_hdinsight_spark_cluster" "spark" {
  name                = "hdi-${var.appname}-${var.environment}-spark"
  resource_group_name = local.resource_group_name
  location            = local.location
  cluster_version     = "4.0"
  tier                = "Standard"

  component_version {
    spark = "2.4"
  }

  gateway {
    enabled  = true
    username = var.hdinsight_user
    password = local.hdinsight_password
  }

  storage_account {
    storage_container_id = azurerm_storage_container.spark.id
    storage_account_key  = azurerm_storage_account.hdinsight.primary_access_key
    is_default           = true
  }

  roles {
    head_node {
      vm_size  = "Standard_D3_V2"
      virtual_network_id = azurerm_virtual_network.main.id
      subnet_id = azurerm_subnet.hdinsight.id
      username = var.ssh_user
      password = local.vm_password
    }

    worker_node {
      vm_size               = "Standard_D3_V2"
      virtual_network_id = azurerm_virtual_network.main.id
      subnet_id = azurerm_subnet.hdinsight.id
      username              = var.ssh_user
      password              = local.vm_password
      target_instance_count = 3
    }

    zookeeper_node {
      vm_size  = "Medium"
      virtual_network_id = azurerm_virtual_network.main.id
      subnet_id = azurerm_subnet.hdinsight.id
      username = var.ssh_user
      password = local.vm_password
    }
  }
}

# Create HDInsight Spark edge node

resource "azurerm_template_deployment" "edge" {
  name                     = "hdi-${var.appname}-${var.environment}-edge"
  resource_group_name = local.resource_group_name

  template_body = <<DEPLOY
{
  "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
  "contentVersion": "1.0.0.0",
  "parameters": {
    "clusterName": {
      "type": "string",
      "metadata": {
        "description": "The name of the HDInsight cluster on which to deploy the edge node. The cluster must exist."
      }
    },
    "provisioningScript": {
      "type": "string",
      "metadata": {
        "description": "The script to run on the edge node after provisioning. The default value points to an empty script."
      },
      "defaultValue": "https://raw.githubusercontent.com/Azure/azure-quickstart-templates/master/101-hdinsight-linux-add-edge-node/scripts/EmptyNodeSetup.sh"
    }
  },
  "variables": {
    "applicationName": "new-edgenode"
  },
  "resources": [
    {
      "name": "[concat(parameters('clusterName'),'/', variables('applicationName'))]",
      "type": "Microsoft.HDInsight/clusters/applications",
      "apiVersion": "2015-03-01-preview",
      "dependsOn": [],
      "properties": {
        "marketPlaceIdentifier": "EmptyNode",
        "computeProfile": {
          "roles": [
            {
              "name": "edgenode",
              "targetInstanceCount": 1,
              "hardwareProfile": {
                "vmSize": "Standard_D3_v2"
              }
            }
          ]
        },
        "installScriptActions": [
          {
            "name": "[concat('emptynode','-' ,uniquestring(variables('applicationName')))]",
            "uri": "[parameters('provisioningScript')]",
            "roles": [
              "edgenode"
            ]
          }
        ],
        "uninstallScriptActions": [],
        "httpsEndpoints": [],
        "applicationType": "CustomApplication"
      }
    }
  ],
  "outputs": {
    "application": {
      "type": "object",
      "value": "[reference(resourceId('Microsoft.HDInsight/clusters/applications/',parameters('clusterName'), variables('applicationName')))]"
    }
  }
}
DEPLOY

  parameters = {
    clusterName = azurerm_hdinsight_spark_cluster.spark.name
  }

  deployment_mode = "Incremental"
}

# Create Kafka cluster

resource "azurerm_storage_container" "kafka" {
  name                  = "hdinsightkafka"
  storage_account_name  = azurerm_storage_account.hdinsight.name
  container_access_type = "private"
}


resource "azurerm_hdinsight_kafka_cluster" "kafka" {
  name                = "hdkafka-${var.appname}-${var.environment}"
  resource_group_name = local.resource_group_name
  location            = local.location
  cluster_version     = "4.0"
  tier                = "Standard"

  component_version {
    kafka = "2.1"
  }

  gateway {
    enabled  = true
    username = var.hdinsight_user
    password = local.hdinsight_password
  }

  storage_account {
    storage_container_id = azurerm_storage_container.kafka.id
    storage_account_key  = azurerm_storage_account.hdinsight.primary_access_key
    is_default           = true
  }

  roles {
    head_node {
      vm_size  = "Standard_D3_V2"
      virtual_network_id = azurerm_virtual_network.main.id
      subnet_id = azurerm_subnet.hdinsight.id
      username = var.ssh_user
      password = local.vm_password
    }

    worker_node {
      vm_size                  = "Standard_D3_V2"
      virtual_network_id = azurerm_virtual_network.main.id
      subnet_id = azurerm_subnet.hdinsight.id
      username                 = var.ssh_user
      password                 = local.vm_password
      number_of_disks_per_node = 3
      target_instance_count    = 3
    }

    zookeeper_node {
      vm_size  = "Standard_D1_V2"
      virtual_network_id = azurerm_virtual_network.main.id
      subnet_id = azurerm_subnet.hdinsight.id
      username = var.ssh_user
      password = local.vm_password
    }
  }
}

# Create jumphost VM

resource "azurerm_public_ip" "jumphostvm" {
  name                = "pip-${var.appname}-${var.environment}-jumphostvm"
  resource_group_name = local.resource_group_name
  location            = local.location
  allocation_method   = "Static"
}


resource "azurerm_network_interface" "jumphostvm" {
  name                = "nic-${var.appname}-${var.environment}-jumphostvm"
  resource_group_name = local.resource_group_name
  location            = local.location

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.jumphost.id
    public_ip_address_id          = azurerm_public_ip.jumphostvm.id
    private_ip_address_allocation = "Dynamic"
  }
}

resource "azurerm_linux_virtual_machine" "jumphostvm" {
  name                = "vm-${var.appname}-${var.environment}-jumphostvm"
  resource_group_name = local.resource_group_name
  location            = local.location
  size                = "Standard_F2"
  admin_username      = var.ssh_user
  network_interface_ids = [
    azurerm_network_interface.jumphostvm.id,
  ]

  admin_ssh_key {
    username   = var.ssh_user
    public_key = local.ssh_pub_key
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "UbuntuServer"
    sku       = "18.04-LTS"
    version   = "latest"
  }
}

# Create VPN Gateway

resource "azurerm_public_ip" "gateway" {
  name                = "pip-${var.appname}-${var.environment}-gateway"
  location            = local.location
  resource_group_name = local.resource_group_name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_virtual_network_gateway" "gateway" {
  name                = "gw-${var.appname}-${var.environment}"
  location            = local.location
  resource_group_name = local.resource_group_name

  type     = "Vpn"
  vpn_type = "RouteBased"

  active_active = false
  enable_bgp    = false
  sku           = "VpnGw1"

  ip_configuration {
    name                          = "vnetGatewayConfig"
    public_ip_address_id          = azurerm_public_ip.gateway.id
    private_ip_address_allocation = "Dynamic"
    subnet_id                     = azurerm_subnet.gateway.id
  }

  vpn_client_configuration {
    address_space = ["10.2.0.0/24"]

    vpn_client_protocols = [
              "IkeV2",
              "OpenVPN",
    ]

    root_certificate {
      name = "VPN-CA"
      public_cert_data = local.vpn_cert_data
    }
  }
}

# Create container instances

resource "azurerm_network_profile" "helpers" {
  name                = "nprof-${var.appname}-${var.environment}"
  location            = local.location
  resource_group_name = local.resource_group_name

  container_network_interface {
    name = "cnic"

    ip_configuration {
      name      = "ipconfig"
      subnet_id = azurerm_subnet.containers.id
    }
  }
}


resource "azurerm_container_group" "helpers" {
  name                = "aci-${var.appname}-${var.environment}-helpers"
  resource_group_name = local.resource_group_name
  location            = local.location
  ip_address_type     = "Private"
  network_profile_id  = azurerm_network_profile.helpers.id
  os_type             = "Linux"

  container {
    name   = "redis"
    image  = "redis"
    cpu    = "0.5"
    memory = "1.5"

    ports {
      port     = 6379
      protocol = "TCP"
    }
  }

  container {
    name   = "postgres"
    image  = "postgres:12-alpine"
    cpu    = "0.5"
    memory = "1.5"

    ports {
      port     = 5432
      protocol = "TCP"
    }
    secure_environment_variables = {
     POSTGRES_PASSWORD = "password"
    } 
  }

}

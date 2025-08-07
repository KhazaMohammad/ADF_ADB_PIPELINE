dbutils.fs.mount(
  source = "wasbs://bronze@adventureworks0011.blob.core.windows.net/",
  mount_point = "/mnt/adventureworks/bronze",
  extra_configs = {
    "fs.azure.account.key.adventureworks0011.blob.core.windows.net": "<key_Vault>"
  }
)

dbutils.fs.mount(
  source = "wasbs://silver@adventureworks0011.blob.core.windows.net/",
  mount_point = "/mnt/adventureworks/silver",
  extra_configs = {
    "fs.azure.account.key.adventureworks0011.blob.core.windows.net": "<key_Vault>"
  }
)

dbutils.fs.mount(
  source = "wasbs://gold@adventureworks0011.blob.core.windows.net/",
  mount_point = "/mnt/adventureworks/gold",
  extra_configs = {
    "fs.azure.account.key.adventureworks0011.blob.core.windows.net": "<key_Vault>"
  }
)

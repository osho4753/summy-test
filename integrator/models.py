from django.db import models


class ProductSyncState(models.Model):
    """
    We store only the hash of the last synced data to detect changes.
    """
    sku = models.CharField(max_length=255, unique=True)
    data_hash = models.CharField(max_length=64) 
    last_synced = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Product Sync State"
        verbose_name_plural = "Product Sync States"

    def __str__(self):
        return f"{self.sku} - {self.last_synced}"

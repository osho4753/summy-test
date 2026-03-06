from django.contrib import admin
from .models import ProductSyncState


@admin.register(ProductSyncState)
class ProductSyncStateAdmin(admin.ModelAdmin):
    list_display = ('sku', 'data_hash', 'last_synced')
    search_fields = ('sku',)
    readonly_fields = ('last_synced',)

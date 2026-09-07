#!/usr/bin/env python3
"""给 SeaTunnel CDC connector jar 打 SPI 补丁(与 PG/SQLServer 相同):
1. 裁剪 Factory SPI: 只留本连接器 source factory + 对应 jdbc catalog factory
   (混入的无关工厂会在 ServiceLoader 遍历时因缺类中断, 导致后面的 catalog 永远找不到)
2. 补 CatalogFactory SPI: 注册 jdbc catalog factory(CDC source 初始化需要 catalog 发现表结构)
用法: python3 patch_cdc_spi.py <jar路径> <SourceFactory类> <CatalogFactory类>
"""
import sys, zipfile, shutil, os

jar, src_factory, cat_factory = sys.argv[1], sys.argv[2], sys.argv[3]
FACTORY_SPI = "META-INF/services/org.apache.seatunnel.api.table.factory.Factory"
CATALOG_SPI = "META-INF/services/org.apache.seatunnel.api.table.factory.CatalogFactory"

tmp = jar + ".tmp"
written = set()
with zipfile.ZipFile(jar, "r") as zin, zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zout:
    for item in zin.infolist():
        data = zin.read(item.filename)
        if item.filename == FACTORY_SPI:
            data = (src_factory + "\n" + cat_factory + "\n").encode()
            written.add(FACTORY_SPI)
        elif item.filename == CATALOG_SPI:
            data = (cat_factory + "\n").encode()
            written.add(CATALOG_SPI)
        zout.writestr(item, data)
    # 新增不存在的 SPI 文件
    if CATALOG_SPI not in written:
        zout.writestr(CATALOG_SPI, (cat_factory + "\n").encode())

shutil.move(tmp, jar)
print(f"patched {jar}")
with zipfile.ZipFile(jar) as z:
    print("  Factory SPI:", z.read(FACTORY_SPI).decode().strip().replace("\n", ", "))
    print("  CatalogFactory SPI:", z.read(CATALOG_SPI).decode().strip())

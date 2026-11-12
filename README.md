# Databricks Medallion Delta Pipeline

### Tablas Delta en Unity Catalog, reglas de calidad idempotentes, Lakeflow Jobs y Spark Declarative Pipelines
*Proyecto académico — Ingeniería de Datos, Universidad EIA (2026-2)*

![Databricks](https://img.shields.io/badge/Databricks-Free_Edition-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-ACID-00ADD4?style=for-the-badge&logo=delta&logoColor=white)
![Apache Spark](https://img.shields.io/badge/PySpark-DataFrames-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Governance-1B3139?style=for-the-badge&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-Databricks_SQL-4479A1?style=for-the-badge&logo=databricks&logoColor=white)

Este repositorio implementa un flujo de datos de ventas sobre **Databricks Free Edition** con arquitectura medallón (bronze → silver → gold). Los datos se guardan como tablas **Delta** gobernadas por **Unity Catalog**, se limpian con reglas de calidad que se pueden volver a ejecutar sin duplicar filas, se orquestan con un **Job** de dos tareas (incluyendo un fallo controlado) y se recrean de forma declarativa con un **pipeline** de dos vistas materializadas.

---

## Tabla de contenido

- [Alcance](#alcance)
- [Estructura del repositorio](#estructura-del-repositorio)
- [Requisitos previos](#requisitos-previos)
- [Ejercicio 1 — Tablas Delta en Unity Catalog](#ejercicio-1--tablas-delta-en-unity-catalog)
- [Ejercicio 2 — Transformación y reglas de calidad](#ejercicio-2--transformación-y-reglas-de-calidad)
- [Ejercicio 3 — Job con dos tareas y fallo controlado](#ejercicio-3--job-con-dos-tareas-y-fallo-controlado)
- [Ejercicio 4 — Pipeline declarativo](#ejercicio-4--pipeline-declarativo)
- [Cómo reproducirlo](#cómo-reproducirlo)
- [Tecnologías](#tecnologías)
- [Contexto académico](#contexto-académico)

---

## Alcance

| Ejercicio | Qué se demuestra | Objetos creados |
|:---:|---|---|
| 1 | Crear y consultar tablas Delta con nombre de tres niveles | `ventas_bronze` |
| 2 | Deduplicar, aplicar reglas de calidad y agregar, de forma idempotente | `ventas_silver`, `ventas_por_categoria` |
| 3 | Orquestar dos notebooks con dependencia y diagnosticar un fallo | `job_ventas_juanjo` |
| 4 | Reemplazar el flujo manual por un pipeline declarativo | `pipeline_ventas_limpias`, `pipeline_ventas_por_categoria` |

Catálogo y esquema usados: `workspace.ingenieria_datos_juanjo`.

---

## Estructura del repositorio

```
.
├── README.md
├── .gitignore
│
├── notebooks/
│   ├── 00_crear_esquema.ipynb           # CREATE SCHEMA en Unity Catalog
│   ├── 01_cargar_ventas.ipynb           # Crea el DataFrame y guarda ventas_bronze
│   ├── 02_transformar_ventas.ipynb      # Limpieza, calidad y resumen por categoría
│   ├── 03_consulta_ventas.ipynb         # Lectura de la tabla desde otro notebook
│   ├── 04_agregar_venta_pipeline.ipynb  # INSERT directo en bronze para probar el pipeline
│   └── 05_consulta_pipeline.ipynb       # Consulta del resultado del pipeline
│
├── pipeline/
│   └── pipeline_ventas.py               # Dos vistas materializadas (Spark Declarative Pipelines)
│
└── docs/
    └── Taller Databricks - Juan Jose Jaramillo Mora.pdf   # Informe con capturas y respuestas
```

Los notebooks están en formato Jupyter (`.ipynb`) exportados desde Databricks, por lo que GitHub los muestra con sus resultados y se pueden volver a importar al workspace.

---

## Requisitos previos

- Cuenta de **Databricks Free Edition** (cómputo serverless).
- Permiso para crear esquemas en el catálogo `workspace`.
- No se necesitan credenciales ni claves: todo corre dentro del workspace.

---

## Ejercicio 1 — Tablas Delta en Unity Catalog

`01_cargar_ventas.ipynb` define el esquema de forma explícita, crea un DataFrame de **17 filas** (16 ventas distintas más una repetida a propósito, además de una venta sin producto y otra con cantidad negativa) y lo guarda como tabla Delta administrada:

```python
(df.write.format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable("workspace.ingenieria_datos_juanjo.ventas_bronze"))
```

`03_consulta_ventas.ipynb` lee la misma tabla desde otro notebook usando solo el nombre completo `catálogo.esquema.tabla`, lo que muestra que el dato vive en el catálogo y no en la sesión.

| Concepto | DataFrame de Spark | Tabla Delta |
|---|---|---|
| Persistencia | Solo en la sesión | Guardada en almacenamiento |
| Almacenamiento | Memoria / cómputo | Parquet + `_delta_log` |
| Transacciones | No | ACID |
| Historial y versiones | No | Sí (`DESCRIBE HISTORY`) |

---

## Ejercicio 2 — Transformación y reglas de calidad

`02_transformar_ventas.ipynb` aplica, en orden:

1. Lectura de `ventas_bronze`.
2. `dropDuplicates()` para quitar duplicados idénticos.
3. Reglas de calidad: `producto` no nulo ni vacío, `cantidad > 0` y `precio_unitario > 0`.
4. Columna calculada `total_venta = cantidad * precio_unitario`.
5. Guardado de `ventas_silver` y del resumen `ventas_por_categoria`.

**Resultados verificados**

| Etapa | Registros | Descartados |
|---|:---:|---|
| Bronze | 17 | — |
| Sin duplicados | 16 | 1 duplicado |
| Silver (reglas de calidad) | 14 | 2 (sin producto, cantidad negativa) |

| Categoría | Total vendido | Ventas |
|---|---:|:---:|
| Electrónica | 9.290.000 | 6 |
| Ropa | 1.450.000 | 4 |
| Accesorios | 1.295.000 | 4 |
| **Total** | **12.035.000** | **14** |

**Idempotencia:** las escrituras usan `mode("overwrite")`, así que ejecutar el notebook varias veces reemplaza la tabla en lugar de acumular filas. Se comprobó con `DESCRIBE HISTORY`, que registra una versión por ejecución y mantiene los mismos conteos.

---

## Ejercicio 3 — Job con dos tareas y fallo controlado

El job `job_ventas_juanjo` encadena dos tareas sobre cómputo serverless:

```
cargar_ventas  ──►  transformar_ventas
(01_cargar_ventas)   (02_transformar_ventas, Run if: All succeeded)
```

- **Ejecución correcta:** las dos tareas terminan en *Succeeded*.
- **Fallo controlado:** se agregó temporalmente en `01_cargar_ventas` una columna inexistente:

  ```python
  df.select(F.col("columna_inexistente")).show()
  ```

  `cargar_ventas` queda en *Failed* y `transformar_ventas` en *Upstream failed*, es decir, no se ejecuta con datos incompletos. La causa se identifica en la pestaña **Runs** (mensaje de error y celda que falló).
- **Recuperación:** se retiró la línea y se volvió a ejecutar; las dos tareas terminaron bien.

---

## Ejercicio 4 — Pipeline declarativo

`pipeline/pipeline_ventas.py` declara **qué** se quiere obtener, y Databricks resuelve el orden y las dependencias:

```
ventas_bronze ──► pipeline_ventas_limpias ──► pipeline_ventas_por_categoria
   (Delta)          (14 registros)                  (3 registros)
```

Con `04_agregar_venta_pipeline.ipynb` se insertó una venta nueva directamente en bronze, sin volver a ejecutar el notebook de carga:

```sql
INSERT INTO workspace.ingenieria_datos_juanjo.ventas_bronze
VALUES (17, DATE'2026-10-09', 'Disco duro externo', 'Accesorios', 2, 300000.0);
```

Al actualizar el pipeline, solo cambió la categoría **Accesorios** (de 1.295.000 a 1.895.000 y de 4 a 5 ventas), y `05_consulta_pipeline.ipynb` lo confirma.

| | Job (ejercicio 3) | Pipeline (ejercicio 4) |
|---|---|---|
| Enfoque | Imperativo: se define el orden de las tareas | Declarativo: se definen los resultados |
| Dependencias | Se configuran a mano | Se infieren de las lecturas entre tablas |
| Unidad de trabajo | Notebooks | Vistas materializadas |

---

## Cómo reproducirlo

1. En el workspace, importar los archivos de `notebooks/` (Workspace → Import → archivos `.ipynb`).
2. Ejecutar `00_crear_esquema` y luego `01_cargar_ventas` y `02_transformar_ventas`.
3. Ejecutar `03_consulta_ventas` para validar la lectura por nombre completo.
4. Crear un job con dos tareas (`01` → `02`) y ejecutarlo, también con el fallo controlado descrito arriba.
5. Crear un pipeline (Jobs & Pipelines → ETL pipeline) usando `pipeline/pipeline_ventas.py` como código fuente y ejecutarlo.
6. Ejecutar `04_agregar_venta_pipeline`, actualizar el pipeline y consultar con `05_consulta_pipeline`.

---

## Tecnologías

| Categoría | Herramientas |
|---|---|
| Plataforma | Databricks Free Edition (serverless) |
| Almacenamiento | Delta Lake (Parquet + `_delta_log`) |
| Gobierno de datos | Unity Catalog (`catálogo.esquema.tabla`) |
| Procesamiento | Apache Spark / PySpark (DataFrames) |
| Orquestación | Lakeflow Jobs, Lakeflow Spark Declarative Pipelines |
| Lenguajes | Python, SQL |

---

## Contexto académico

Taller individual del curso **Ingeniería de Datos** de la **Universidad EIA**, periodo **2026-2**. Evalúa el uso de tablas Delta en Unity Catalog, la aplicación de reglas de calidad con ejecución idempotente, la orquestación con jobs y el diseño declarativo con pipelines.

**Autor**

- Juan José Jaramillo Mora — [@Juanjo1414](https://github.com/Juanjo1414)
# Databricks notebook source
# DBTITLE 1,Leer configuraciones
# Read Secret Scope
secret_scope__df = 'dashbi'
dbutils.widgets.text('secret_scope',secret_scope__df,'01. Nombre secret scope')
secret_scope_fn = dbutils.widgets.get('secret_scope')
secret_scope = (
    None
    if secret_scope_fn is None or secret_scope_fn.strip() == ''
    else secret_scope_fn.strip()
)
# Read Secret Prefix
secret_prefix__df = 'db-mysql-portal-noprod'
dbutils.widgets.text('secret_prefix',secret_prefix__df,'02. Nombre secret prefix')
secret_prefix_fn = dbutils.widgets.get('secret_prefix')
secret_prefix = (
    None
    if secret_prefix_fn is None or secret_prefix_fn.strip() == ''
    else secret_prefix_fn.strip()
)
# Read Catalog Name
dbutils.widgets.text('catalog_name','portal_qa','03. Nombre del catálogo')
catalog_name_fn = dbutils.widgets.get('catalog_name')
catalog_name = (
    None
    if catalog_name_fn is None or catalog_name_fn.strip() == ''
    else catalog_name_fn.strip()
)
# Read Database Name.


dbutils.widgets.text('dbname','handytec_portal_test','04. Nombre base de datos')
dbname_fn = dbutils.widgets.get('dbname')
dbname = (
    None
    if dbname_fn is None or dbname_fn.strip() == ''
    else dbname_fn.strip()
)

# COMMAND ----------

# DBTITLE 1,Cargar variables
# MAGIC %run ./00_config_parameters

# COMMAND ----------

# DBTITLE 1,Cargar utilidades
# MAGIC %run ./00_helpers

# COMMAND ----------

# DBTITLE 1,Leer configuraciones BDD
leave_balance_table = 'app_employee_leave_balance'
employee_table = 'app_employee'

ERROR_LOG_PATH = "/tmp/employes_not_found_leave_balance.txt"
# El formato flexible 'd' acepta '1' o '01'
DATE_FORMAT = 'yyyy-MM-dd'

LEAVE_BALANCE_COLUMN_MAPPING = {
    "Email_Empleado": "emailInput",
    "Fecha_Inicio_Periodo": "dateInit",
    "Fecha_Fin_Periodo": "dateEnd",
    "Dias_Asignados": "daysPeriod",
    "Dias_Tomados": "daysTaken",
    "Dias_Disponibles": "daysAvailable",
    "Estado_Registro": "state"
}
TEMP_DELETE_TABLE = "temp_vacation_keys_to_delete"

# COMMAND ----------

# DBTITLE 1,Imprimir configuraciones utilizadas
debug_message(f"{LogColors.BLUE}Resumen configuraciones aplicadas:")
debug_message(f"{LogColors.BLUE}{secret_scope} | {secret_prefix} | {catalog_name} | {dbname} {LogColors.RESET}")

# COMMAND ----------

# DBTITLE 1,Obtener path del archivo
filepath, filename = get_file_from_storage_with_keys("plantilla","handytec")
except_content_msg = "No se pudo obtener el archivo con los datos de las vacaciones"
if filepath is None:
    raise FileNotFoundError(except_content_msg)


# COMMAND ----------

# DBTITLE 1,Leer el archivo excel del volumen mensual
try: 
    df_balance_template = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .load(filepath)
except Exception as e:
    debug_error(f"Error al leer el archivo Excel: {e}")

# COMMAND ----------

# DBTITLE 1,Renombrar columnas
df_balance = df_balance_template
# Itera sobre el diccionario y aplica el cambio
for old_name, new_name in LEAVE_BALANCE_COLUMN_MAPPING.items():
    df_balance = df_balance.withColumnRenamed(old_name, new_name)

# COMMAND ----------

# DBTITLE 1,Seleccionar columnas necesarias y formatear fecha
# Seleccionar columnas requeridas
df_balance = df_balance.select(
    col(LEAVE_BALANCE_COLUMN_MAPPING["Email_Empleado"]),
    col(LEAVE_BALANCE_COLUMN_MAPPING["Fecha_Inicio_Periodo"]).alias("dateInit"),
    col(LEAVE_BALANCE_COLUMN_MAPPING["Fecha_Fin_Periodo"]).alias("dateEnd"),
    col(LEAVE_BALANCE_COLUMN_MAPPING["Dias_Asignados"]).alias("daysPeriod").cast("int"),
    col(LEAVE_BALANCE_COLUMN_MAPPING["Dias_Tomados"]).alias("daysTaken").cast("int"),
    col(LEAVE_BALANCE_COLUMN_MAPPING["Dias_Disponibles"]).alias("daysAvailable").cast("int"),
    col(LEAVE_BALANCE_COLUMN_MAPPING["Estado_Registro"]).alias("state").cast("boolean")
)

# Formatear la fecha al estándar de la BDD
period_start_infered = F.to_date(
    col(LEAVE_BALANCE_COLUMN_MAPPING["Fecha_Inicio_Periodo"])
)
period_end_infered = F.to_date(
    col(LEAVE_BALANCE_COLUMN_MAPPING["Fecha_Fin_Periodo"])
)

# Colocar valor adecuado para las fechas
df_balance = df_balance.withColumn(
    LEAVE_BALANCE_COLUMN_MAPPING["Fecha_Inicio_Periodo"],
    period_start_infered
).withColumn(
    LEAVE_BALANCE_COLUMN_MAPPING["Fecha_Fin_Periodo"],
    period_end_infered
)

# COMMAND ----------

# DBTITLE 1,Filtrado y validación de integridad en fechas
# Eliminamos filas que sean idénticas en usuario y fechas
df_balance = df_balance.dropDuplicates(["emailInput", "dateInit", "dateEnd"])

# Definición de validaciones básicas (Integridad)
period_integrity = (col("dateInit").isNotNull()) & \
                   (col("dateEnd").isNotNull()) & \
                   (col("dateInit") < col("dateEnd"))
integrity_msg = (
    "Error de integridad: "
    "Verifique que la fecha de inicio sea menor a la de fin. "
    "Además que las fechas no sean nulas"
)
df_error_integrity = df_balance.filter(~period_integrity) \
    .withColumn("Error_Reason", lit(integrity_msg))

# COMMAND ----------

# DBTITLE 1,Filtrar registros con fechas de periodo inválidas
# Separamos los registros con fechas inválidas (tu validación actual)
df_invalido_basico = df_error_integrity

df_temp = df_balance.filter(period_integrity)
df_temp = df_temp.withColumn("tmp_id", F.monotonically_increasing_id())

# Self-Join para comparar periodos del mismo emailInput
# Se usa alias 'a' y 'b' para referirnos a la misma tabla
df_overlaps = df_temp.alias("a").join(
    df_temp.alias("b"),
    (col("a.emailInput") == col("b.emailInput")) &
    (col("a.tmp_id") != col("b.tmp_id")) &
    (col("a.dateInit") <= col("b.dateEnd")) &
    (col("b.dateInit") <= col("a.dateEnd")),
    "left"
)

# Sub-periodo: El periodo A está totalmente contenido dentro del periodo B
is_sub_period = (
    (col("a.dateInit") >= col("b.dateInit")) &
    (col("a.dateEnd") <= col("b.dateEnd"))
)

# Solape parcial: Las fechas se cruzan pero no hay uno contenido en otro
is_partial_overlap = (
    (col("a.dateInit") <= col("b.dateEnd")) &
    (col("b.dateInit") <= col("a.dateEnd")) &
    ~is_sub_period
)

# Clasificamos los errores
sub_period_msg = (
    "Redundancia detectada: El periodo nuevo es un sub-conjunto "
    "de un registro previo (No se requiere actualización)."
)

overlap_msg = (
    "Error de integridad: El periodo choca parcialmente con la "
    "historia del usuario. El registro no es totalmente envolvente."
)
df_classified = df_overlaps.withColumn(
    "temp_error",
    F.when(is_sub_period, lit(sub_period_msg))
     .when(is_partial_overlap, lit(overlap_msg))
)

# Agrupamos para obtener un solo registro con su error más grave si tiene varios solapes
df_errors_overlap = df_classified.filter(col("temp_error").isNotNull()) \
    .groupBy("a.emailInput", "a.dateInit", "a.dateEnd") \
    .agg(F.max("temp_error").alias("Error_Reason"))

# COMMAND ----------

# DBTITLE 1,Unir errores y filtrar registros válidos
# Unir todos los errores (basicos + solapamientos)
df_with_invalid_records = df_invalido_basico \
.select("emailInput", "dateInit", "dateEnd", "Error_Reason") \
.unionByName(df_errors_overlap)

# El DataFrame Válido son los registros que NO están en la tabla de errores
df_balance_valid = df_temp.join(
    df_errors_overlap, 
    ["emailInput", "dateInit", "dateEnd"],
    "left_anti"
).drop("tmp_id")

# COMMAND ----------

# DBTITLE 1,Generar clave de periodo
df_balance = (
    df_balance_valid
    .withColumn("yearInit", F.year("dateInit"))
    .withColumn(
        "period_key",
        F.concat_ws("|",
            F.col("dateInit").cast("string"), F.col("dateEnd").cast("string")
        )
    )
)

# COMMAND ----------

# DBTITLE 1,Obtener grupos inválidos
window_year = Window.partitionBy("emailInput", "yearInit")
email_error_msg = (
    "Información incompleta: El registro no tiene "
    "el correo del empleado o el año de referencia."
)

duplicated_record_msg = (
    "Registro duplicado: Este empleado ya tiene "
    "información registrada para el año seleccionado."
)

df_classified_year = df_balance.withColumn(
    "rows_in_group", F.count("*").over(window_year)
).withColumn(
    "Error_Reason",
    F.when(
        (F.col("emailInput").isNull()) | (F.col("yearInit").isNull()),
        F.lit(email_error_msg)
    )
    .when(
        F.col("rows_in_group") > 1,
        F.lit(duplicated_record_msg)
    )
    .otherwise(F.lit(None))
)
# Separar los nuevos errores detectados
df_invalido_year = df_classified_year.filter(F.col("Error_Reason").isNotNull()) \
    .select("emailInput", "dateInit", "dateEnd", "Error_Reason")

# Integrar con el DF inválido
df_with_invalid_records = df_with_invalid_records.unionByName(df_invalido_year)
df_balance = df_classified_year.filter(
    F.col("Error_Reason").isNull()
).drop("rows_in_group", "Error_Reason")

# COMMAND ----------

# DBTITLE 1,Obtener emails válidos
# Obtenemos los datos de Spark
distinct_emails = df_balance.select("emailInput").distinct().collect()
# Formateamos la lista de Python
emails_list = [f"'{row.emailInput}'" for row in distinct_emails]
emails_str = ",".join(emails_list)

# COMMAND ----------

# DBTITLE 1,Consultar balance de la bdd de datos
employee_balance_query = f"""
select 
    b.email as emailInput,
    a.dateInit,
    a.dateEnd
from
    `{catalog_name}`.`{dbname}`.`{leave_balance_table}` a
    LEFT JOIN `{catalog_name}`.`{dbname}`.`{employee_table}` b
    on a.employeeId = b.employeeId
WHERE
    b.email IN ({emails_str})
"""
df_balance_from_db = spark.sql(employee_balance_query)

# COMMAND ----------

# DBTITLE 1,Gestionar choques y validar registros para inserción
new_df = df_balance.alias("new")
db_df = df_balance_from_db.alias("db")

# Identificar TODOS los choques (Cross-over de fechas)
df_all_clashes = new_df.join(
    db_df,
    (F.col("new.emailInput") == F.col("db.emailInput")) &
    (F.col("new.dateInit") <= F.col("db.dateEnd")) &
    (F.col("db.dateInit") <= F.col("new.dateEnd")),
    "inner"
)

# Clasificar los choques
df_classified = df_all_clashes.select(
    "new.*",
    F.col("db.dateInit").alias("db_dateInit"),
    F.col("db.dateEnd").alias("db_dateEnd"),
    F.when(
        (F.col("new.dateInit") <= F.col("db.dateInit")) &
        (F.col("new.dateEnd") >= F.col("db.dateEnd")),
        "SUPREMACIA"
    ).otherwise("SOLAPE_PARCIAL").alias("clash_type")
)

# Registros de la DB que deben ELIMINARSE
df_db_to_delete = df_classified.filter(F.col("clash_type") == "SUPREMACIA") \
    .select(
        "emailInput",
        F.col("db_dateInit").alias("dateInit"), F.col("db_dateEnd").alias("dateEnd")
    ) \
    .distinct()

overlap_error_msg = (
    "Conflicto de integridad: Se detectó un solapamiento parcial. "
    "El nuevo periodo choca con registros existentes sin cubrirlos totalmente."
)
# Solapes parciales que no envuelven totalmente (riesgo de inconsistencia)
df_overlap_errors = df_classified.filter(F.col("clash_type") == "SOLAPE_PARCIAL") \
    .select("emailInput", "dateInit", "dateEnd") \
    .withColumn("Error_Reason", F.lit(overlap_error_msg)) \
    .distinct()

# Registros NUEVOS VÁLIDOS para insertar
df_limpios = df_balance.join(
    df_all_clashes.select("new.emailInput", "new.dateInit", "new.dateEnd").distinct(),
    ["emailInput", "dateInit", "dateEnd"],
    "left_anti"
)

# Registros que chocaron pero ganaron (Supremacía)
df_ganadores = df_classified.filter(F.col("clash_type") == "SUPREMACIA") \
    .join(df_overlap_errors, ["emailInput", "dateInit", "dateEnd"], "left_anti") \
    .select(new_df.columns) \
    .distinct()

df_with_invalid_records = df_with_invalid_records.unionByName(df_overlap_errors)
# Unión final de lo que sí se puede insertar
df_final_balance_to_insert = df_limpios.unionByName(
    df_ganadores,
    allowMissingColumns=True
)

# COMMAND ----------

# DBTITLE 1,Consultar listado de colaboradores
employee_query = f"""
select
    employeeId,
    trim(email) as email
from
    `{catalog_name}`.`{dbname}`.`{employee_table}`
"""
df_employee = spark.sql(employee_query)

# COMMAND ----------

# DBTITLE 1,Unir los datos de vacaciones y colaboradores
df_balance_employee = df_final_balance_to_insert.join(
    df_employee,
    df_final_balance_to_insert["emailInput"] == df_employee["email"],
    "left_outer"
).drop("email")

# COMMAND ----------

# DBTITLE 1,Filtrar registros con empleados asociados
df_db_to_delete = df_db_to_delete.join(
    df_employee,
    df_db_to_delete["emailInput"] == df_employee["email"],
    "left_outer"
).drop("email")
df_db_to_delete = df_db_to_delete.filter(col("employeeId").isNotNull())


# COMMAND ----------

# DBTITLE 1,Extraer datos válidos e inválidos contra la tabla de empleados
# Registros con employeeId (Emails encontrados)
df_balance_employee_valid = df_balance_employee.filter(col("employeeId").isNotNull())
# Registros sin employeeId (Emails no encontrados)
user_not_found_msg = (
    "Error de identidad: El usuario no existe en el sistema "
    "o no tiene un ID de empleado asociado."
)
df_no_employee_errors = df_balance_employee.filter(col("employeeId").isNull()) \
    .withColumn("Error_Reason", lit(user_not_found_msg))

# 2. Seleccionamos las columnas necesarias para que coincidan con dfInvalido
# Asegúrate de usar los nombres de columnas que definiste anteriormente
df_employee_errors_to_add = df_no_employee_errors.select(
    col("emailInput"),
    col("dateInit"),
    col("dateEnd"),
    col("Error_Reason")
)

# 3. Lo añadimos al DataFrame global de errores
df_with_invalid_records = df_with_invalid_records.unionByName(df_employee_errors_to_add)

# COMMAND ----------

# DBTITLE 1,Seleccionar y ordenar las columnas para el INSERT/UPDATE
df_balance_to_save = df_balance_employee_valid.select(
    col("employeeId").cast("int"),
    col("dateInit"),
    col("dateEnd"),
    col("daysPeriod"),
    col("daysTaken"),
    col("daysAvailable"),
    col("state").cast("int").alias("state"),
    current_timestamp().alias("createdAt"),
    current_timestamp().alias("updatedAt")
)

# COMMAND ----------

# DBTITLE 1,Función De Eliminación De Registros anteriores
def delete_existing_records(df_to_delete: DataFrame):
    """
    Se conecta a la BDD y elimina cualquier registro de balance que coincida
    con los employeeId, dateInit y dateEnd que están en el archivo de carga.
    """
    if not df_to_delete:
        debug_warning("No hay registros válidos para eliminar.")
        return

    # Cargar las claves a eliminar a la tabla temporal de MySQL usando JDBC
    debug_info(f"Cargando claves en la tabla temporal MySQL: {TEMP_DELETE_TABLE}")
    try:
        df_to_delete.write \
            .mode("overwrite") \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", TEMP_DELETE_TABLE) \
            .option("driver", jdbc_driver) \
            .option("user", jdbc_username) \
            .option("password", jdbc_password) \
            .save()
    except Exception as e:
        debug_error(f"No se pudo cargar la tabla temporal MySQL. Error: {e}")
        return

    conn = None
    try:
        conn = jaydebeapi.connect(jdbc_driver, jdbc_url, [jdbc_username, jdbc_password])
        cursor = conn.cursor()
        # Consulta DELETE usando JOIN para MySQL: Elimina de la tabla principal
        # cualquier registro que coincida con la clave
        # (employeeId, dateInit, dateEnd) en la tabla temporal
        delete_query = f"""
            DELETE t1
            FROM {leave_balance_table} t1
            JOIN {TEMP_DELETE_TABLE} t2 ON
                t1.employeeId = t2.employeeId AND
                t1.dateInit = t2.dateInit AND
                t1.dateEnd = t2.dateEnd;
        """
        cursor.execute(delete_query)
        deleted_count = cursor.rowcount
        conn.commit()
        debug_info(f"Eliminados {deleted_count} registros anteriores de la BDD")
    except jaydebeapi.connector.Error as err:
        if conn:
            conn.rollback()
        debug_error(f"Falló la conexión o ejecución del DELETE en MySQL: {err.msg}")
    except Exception as e:
        if conn:
            conn.rollback()
        debug_error(f"Error inesperado: {e}")
    finally:
        if conn:
            # Limpiar la tabla temporal
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {TEMP_DELETE_TABLE}")
                conn.commit()
                debug_info(f"Tabla temporal {TEMP_DELETE_TABLE} eliminada.")
            except Exception as drop_e:
                debug_error(f"[WARNING] No se pudo eliminar la tabla temporal: {drop_e}")
            conn.close()

# COMMAND ----------

# DBTITLE 1,Eliminar registros desactualizados si existen
total_records_delete = df_db_to_delete.count()
if total_records_delete > 0:
    # Recoger las claves únicas (employeeId, dateInit, dateEnd) a eliminar
    old_data_to_delete_df = df_db_to_delete.select("employeeId", "dateInit", "dateEnd")
    # Ejecutar la eliminación de registros
    delete_existing_records(old_data_to_delete_df)
else:
    debug_info("No hay registros desactualizados para eliminar")

# COMMAND ----------

# DBTITLE 1,Eliminar datos anteriores
total_records = df_balance_to_save.count()
if total_records > 0:
    # Recoger las claves únicas (employeeId, dateInit, dateEnd) a eliminar
    previous_data_df = df_balance_to_save.select("employeeId", "dateInit", "dateEnd")
    # Ejecutar la eliminación de registros
    delete_existing_records(previous_data_df)
else:
    debug_info("No hay registros anteriores válidos para eliminar")

# COMMAND ----------

# DBTITLE 1,Guardar datos actualizados
try:
    if total_records > 0:
        debug_info(f"Cargando {total_records} registros en {leave_balance_table}")
        # La carga de datos usando 'append' insertará los nuevos registros
        df_balance_to_save.write \
            .mode("append") \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", leave_balance_table) \
            .option("driver", jdbc_driver) \
            .option("user", jdbc_username) \
            .option("password", jdbc_password) \
            .save()
        debug_info(f"!Carga de datos finalizada! Se guardaron los siguientes registros:")
        df_balance_to_save.show(truncate=False)
    else:
        debug_info("No hay registros válidos para guardar")
except Exception as e:
    debug_error(f"Ocurrió un error al guardar los datos de las vacaciones: {e}")

# COMMAND ----------

# DBTITLE 1,Mostrar registros inválidos
total_invalid_records = df_with_invalid_records.count()
if total_invalid_records > 0:
    debug_info(f"Se encontraron {total_invalid_records} registros inválidos:")
    df_with_invalid_records.show(truncate=False)
else:
    debug_info("No hay registros inválidos")

# COMMAND ----------

# DBTITLE 1,Eliminar archivo procesado
try:
    dbutils.fs.rm(filepath, recurse=True)
    print(f"Eliminado correctamente el archivo procesado: {filepath}")
except Exception as e:
    print(f"Error al eliminar el archivo procesado: {filepath}: {str(e)}")
# Plataforma de Gobierno y Analítica de Datos Jira Cloud
## Especificación Funcional y Arquitectura de la Solución

**Organización:** Gobierno TI - Integratel Perú  
**Entorno Origen:** Atlassian Jira Cloud (`integratelperu.atlassian.net`)  
**Alcance Analítico:** Más de 120 espacios de trabajo activos (Jira Software, Jira Service Management y Jira Work Management), con una volumetría histórica superior a 1.1 millones de tickets y 19 millones de eventos de auditoría (changelog).  
**Destino de Exposición:** Data Warehouse Analítico y Dashboards en Microsoft Power BI.

---

## 1. Resumen Ejecutivo y Objetivos de Negocio

El proyecto implementa una plataforma centralizada de datos diseñada para desacoplar el análisis operativo y estratégico de la API de Atlassian Jira Cloud. La solución extrae periódicamente la actividad de los espacios de trabajo, estructura la información en un almacén analítico relacional y la expone mediante vistas optimizadas para el consumo en herramientas de Inteligencia de Negocios.

```
[ Jira Cloud API ] ---> [ Orquestación Airflow ] ---> [ Data Warehouse PostgreSQL ] ---> [ Microsoft Power BI ]
                                                                  ^
                                                                  |
                                                      [ Portal Web de Gestión ]
```

### Objetivos Estratégicos
1. **Desacoplamiento y Rendimiento:** Reemplazar el consumo directo de APIs o consultas pesadas sobre el sistema transaccional mediante un repositorio analítico local en PostgreSQL 16 con cálculos precomputados de tiempos de ciclo (*Lead Time*, *Cycle Time*) y duraciones en estados.
2. **Gobierno y Normalización de Campos:** Administrar la heterogeneidad de campos personalizados creados en más de 120 proyectos sin generar dispersión de datos (*sparse tables*) con columnas nulas masivas.
3. **Eficiencia en Ingesta:** Ejecutar sincronizaciones incrementales basadas en marcas de agua temporales (*watermarks*) y conciliar eliminaciones masivas mediante consultas ligeras de identificadores con Jira Expressions.
4. **Regularización Histórica sin Consumo de API (*Zero-API Backfill*):** Conservar los registros en bruto en una capa de almacenamiento intermedio (*Staging* JSONB) para habilitar nuevos campos o corregir métricas históricas de forma retroactiva sin volver a consultar a Atlassian.
5. **Operatividad y Seguimiento Diario:** Facilitar un portal web administrativo para el equipo de Gobierno TI que permita auditar la extracción en tiempo real, homologar esquemas y supervisar estadísticas operativas sin requerir intervención en línea de comandos.

---

## 2. Arquitectura Global de la Solución

La plataforma opera en una arquitectura por capas completamente contenerizada bajo Docker, asegurando independencia del entorno operativo y control estricto del consumo de recursos de cómputo y memoria.

### 2.1. Diagrama de Arquitectura por Capas

```mermaid
flowchart TD
    subgraph Origen["1. Capa de Origen de Datos (Jira Cloud)"]
        JIRA_REST["Jira REST API v3<br/>(Búsqueda JQL, Proyectos, Catálogo)"]
        JIRA_EXP["Jira Expressions API<br/>(Conciliación Ligera de Identificadores)"]
        JIRA_SCR["Jira Screen & Scheme API<br/>(Detección de Campos en Pantallas)"]
    end

    subgraph Orquestacion["2. Capa de Orquestación e Ingesta (Apache Airflow)"]
        DAG_INC["Pipeline Incremental<br/>(Extracción Concurrente por Lotes)"]
        DAG_DEL["Pipeline de Reconciliación<br/>(Detección de Tickets Eliminados)"]
        DAG_REC["Pipeline de Mantenimiento<br/>(Regularización y Recálculo)"]
        MONITOR["Monitor de Hilos y Telemetría"]
    end

    subgraph Almacenamiento["3. Capa de Almacenamiento (PostgreSQL 16 DW)"]
        STG["Subcapa Staging (ELT)<br/>- Payloads JSONB Crudos<br/>- Control de Marcas de Agua"]
        CORE["Subcapa Dimensional y Hechos<br/>- Tickets Core Universales<br/>- Extensiones JSM y SLAs<br/>- Historial de Transiciones<br/>- Valores Normalizados (EAV)"]
        SEM["Subcapa Semántica y Datamarts<br/>- Catálogo de Metadatos y Pantallas<br/>- Reglas de Homologación<br/>- Vistas Analíticas Dinámicas (vw_mart_*)"]
    end

    subgraph Operacion["4. Capa de Administración y Operación"]
        UI["Portal Web Operativo (Streamlit)<br/>Monitoreo, Homologación y Estadísticas"]
        CADV["Telemetría de Recursos (cAdvisor)"]
        TUNNEL["Túnel de Acceso Seguro (Ngrok)"]
    end

    subgraph Consumo["5. Capa de Consumo Analítico"]
        PBI["Microsoft Power BI<br/>(Modelos Tabulares y Reportes de Gestión)"]
    end

    Origen --> Orquestacion
    Orquestacion --> Almacenamiento
    Operacion <--> Almacenamiento
    Operacion <--> Orquestacion
    Almacenamiento --> Consumo
```

### 2.2. Descripción de Componentes

* **Capa de Origen (Atlassian Jira Cloud):** Provee los datos transaccionales mediante servicios REST asegurados por tokens de servicio. Emplea endpoints especializados para esquemas de pantalla, consulta paginada de incidencias y expresiones evaluadas en el servidor de Atlassian para extraer lotes masivos de IDs.
* **Capa de Orquestación (Apache Airflow):** Gestiona los flujos de trabajo programados con un ejecutor local multi-proceso (`LocalExecutor`). Coordina la extracción diferencial en paralelo mediante un modelo de productores concurrentes y un consumidor unificado hacia la base de datos.
* **Capa de Almacenamiento (Data Warehouse PostgreSQL 16):** Repositorio relacional central optimizado con extensiones de indexación avanzada (árboles B y GIN sobre JSONB) y segmento de memoria compartida ampliado para procesamiento analítico de alto volumen.
* **Capa de Operación (Portal Web Streamlit):** Interfaz gráfica corporativa para seguimiento de estado, auditoría de hilos de extracción, control de proyectos activos/pausados y administración de reglas de negocio.
* **Capa de Consumo (Power BI):** Acceso nativo SQL a vistas relacionales desnormalizadas donde cada ticket expone sus campos estándar y específicos como columnas directas, eliminando la complejidad del modelo subyacente.

---

## 3. Flujos Funcionales de Datos

### 3.1. Proceso de Sincronización Incremental

```mermaid
sequenceDiagram
    autonumber
    participant Jira as Jira Cloud
    participant Airflow as Orquestador Airflow
    participant Staging as PostgreSQL (Staging)
    participant Core as PostgreSQL (Core DW)
    participant Datamart as Vistas Analíticas

    Airflow->>Jira: 1. Consultar metadatos de proyectos y esquemas de pantallas
    Jira-->>Airflow: Retorna catálogo global y campos activos en pantallas
    Airflow->>Core: Actualizar asociaciones proyecto-campo y reglas base
    Airflow->>Staging: 2. Consultar marcas de agua (Watermarks)
    Airflow->>Jira: 3. Extraer tickets modificados (JQL incremental)
    Jira-->>Airflow: Lotes de tickets con historial de cambios (Changelog)
    Airflow->>Staging: Ingestar JSON crudo en stg_jira_issues_raw
    Airflow->>Core: 4. Transformar datos universales hacia fact_jira_issues_core
    Airflow->>Core: 5. Descomponer campos homologados hacia fact_issue_custom_values
    Airflow->>Core: 6. Reconstruir historial y transiciones de estado
    Airflow->>Datamart: 7. Refrescar vistas analíticas y estadísticas de sincronización
```

1. **Sincronización de Metadatos y Pantallas:** Se cataloga la totalidad de campos de la instancia y se mapean los esquemas de pantalla asignados a cada proyecto activo, descartando campos técnicos o decorativos irrelevantes.
2. **Lectura de Marcas de Agua:** Se identifica la fecha y hora del último ticket sincronizado por proyecto para formular consultas JQL incrementales restringidas a registros modificados recientemente.
3. **Extracción Paralela:** Hilos de trabajo concurrentes solicitan los lotes de datos y su historial de modificaciones a la API de Jira, mitigando los tiempos de respuesta y respetando los límites de tasa de peticiones.
4. **Carga en Staging:** El contenido íntegro se preserva en formato JSONB con trazabilidad de fechas de ingesta y claves únicas inmutables.
5. **Transformación Dimensional:** Se extraen los atributos universales (resumen, estado, tipo, jerarquías, reportero, asignado, fechas clave) poblando el hecho principal.
6. **Normalización EAV y Homologación:** Los campos personalizados asociados a pantallas se procesan según su naturaleza de dato (texto, numérico, fechas, selección en cascada o usuario) y se almacenan en una estructura Entidad-Atributo-Valor tipada.
7. **Línea de Tiempo de Transiciones:** Se recalculan las duraciones de permanencia en cada estado y los tiempos de ciclo para alimentar indicadores de flujo y eficiencia.
8. **Actualización de Vistas:** Se actualizan las vistas del Datamart para reflejar los nuevos campos sin alterar la estructura física de las tablas base.

### 3.2. Conciliación y Detección de Tickets Eliminados

Dado que la API REST de Jira no emite eventos automáticos cuando un registro es eliminado físicamente en la interfaz web, el sistema incorpora un flujo de auditoría ligera:

* **Mecanismo:** Utiliza la API de Jira Expressions para recuperar exclusivamente los identificadores numéricos (`issue_id`) de todos los tickets existentes en cada proyecto mediante una única petición compacta.
* **Conciliación:** Se compara el conjunto de identificadores devueltos contra las claves almacenadas en el Data Warehouse.
* **Borrado Lógico:** Aquellos registros ausentes en la respuesta de Jira se marcan con bandera de eliminación lógica (`is_deleted = TRUE`) registrando la marca de tiempo de detección, lo cual preserva la integridad del historial analítico sin alterar métricas activas.

### 3.3. Regularización Histórica sin Consumo de API (*Zero-API Backfill*)

Cuando una unidad de negocio requiere incorporar un campo personalizado preexistente que no formaba parte del reporte inicial:

* El campo se activa mediante el portal web o la asociación de pantallas.
* Un procedimiento interno de base de datos lee directamente los documentos JSONB almacenados en la capa de *Staging*.
* La información histórica se extrae y se traslada a la tabla dimensional en cuestión de segundos, sin realizar llamadas adicionales a la API de Jira Cloud ni impactar las cuotas de red corporativas.

---

## 4. Arquitectura de Información y Modelo de Datos

El diseño del Data Warehouse sigue un enfoque dimensional estructurado para garantizar consistencia, integridad referencial y alta velocidad de consulta.

```mermaid
erDiagram
    dim_jira_project ||--o{ fact_jira_issues_core : contiene
    dim_jira_project ||--|| meta_project_sync_stats : reporta
    dim_jira_project ||--o{ meta_project_field_association : configura
    dim_jira_user ||--o{ fact_jira_issues_core : asignado
    dim_jira_user ||--o{ fact_jira_issues_core : reportado
    
    fact_jira_issues_core ||--o| fact_jsm_requests : extiende_servicio
    fact_jira_issues_core ||--o{ fact_issue_custom_values : posee_campos
    fact_jira_issues_core ||--o{ fact_jira_changelog : audita_eventos
    fact_jira_issues_core ||--o{ fact_issue_transitions : calcula_tiempos

    meta_jira_field_catalog ||--o{ meta_field_homologation : define_reglas
    meta_jira_field_catalog ||--o{ meta_project_field_association : cataloga
    meta_field_homologation ||--o{ fact_issue_custom_values : tipifica
```

### 4.1. Principios de Integridad y Persistencia

1. **Claves Primarias Inmutables:** Los identificadores de negocio de Jira (`issue_key` como `TI-101` o `project_key` como `TI`) son mutables, ya que un administrador puede renombrar el prefijo de un proyecto o mover una incidencia entre proyectos. Por esta razón, todas las relaciones de integridad referencial se basan en los identificadores numéricos internos inmutables de Jira (`issue_id` y `project_id`).
2. **Normalización Híbrida (Core + EAV):**
   * **Atributos Universales:** Campos compartidos por el 100% de los proyectos (resumen, estado, tipo de incidencia, nivel de jerarquía, creador, asignado, fechas de creación/resolución) residen en columnas dedicadas en la tabla principal.
   * **Atributos Específicos:** Campos propios de proyectos o tipologías particulares se almacenan en una matriz EAV (*Entity-Attribute-Value*) tipada, evitando la proliferación de tablas con cientos de columnas nulas.
3. **Estandarización de Nomenclatura:** Los campos personalizados adoptan de forma unificada el prefijo `cf_` seguido de su nombre canónico en minúsculas y sin caracteres especiales (por ejemplo: `cf_area_cliente`, `cf_moneda`, `cf_prioridad`).

### 4.2. Entidades Principales del Data Warehouse

| Entidad | Tipo | Propósito y Contenido Funcional |
| :--- | :--- | :--- |
| `dim_jira_project` | Dimensión | Maestro de espacios de trabajo, tipo de proyecto (software, servicio, negocio), estado de sincronización y control de cambios de prefijo. |
| `dim_jira_user` | Dimensión | Directorio unificado de usuarios (responsables, solicitantes, aprobadores) con nombres para mostrar, correos y estado en Atlassian. |
| `fact_jira_issues_core` | Hecho Principal | Tabla central de incidencias. Contiene datos universales, niveles de jerarquía (subtarea, estándar, épica), fechas clave, métricas de tiempo global y bandera de borrado lógico. |
| `fact_jsm_requests` | Extensión | Hecho complementario exclusivo para Jira Service Management. Contiene tipos de solicitud del portal, organizaciones de clientes, cumplimiento de SLAs y encuestas de satisfacción. |
| `fact_issue_custom_values` | Hecho EAV | Valores normalizados de campos personalizados por ticket. Almacena representaciones tipadas (texto, numérico, fecha o usuario) sin redundancia. |
| `fact_jira_changelog` | Auditoría | Registro histórico detallado de todas las modificaciones realizadas en los campos de las incidencias (valores anteriores y posteriores, autores y marcas de tiempo). |
| `fact_issue_transitions` | Analítico | Tabla de eventos precomputados que registra cada estado por el que transitó una incidencia, su secuencia cronológica y la duración exacta en segundos. |
| `meta_jira_field_catalog` | Metadatos | Catálogo de la totalidad de campos existentes en la instancia de Jira Cloud, identificando tipos de esquema y proveedores de campos. |
| `meta_project_field_association` | Metadatos | Registro de los campos efectivamente asociados a las pantallas y esquemas de cada proyecto en Jira. |
| `meta_field_homologation` | Metadatos | Reglas de negocio que definen qué campos se incorporan al Datamart, su nombre analítico estandarizado y su estrategia de conversión. |
| `meta_project_sync_stats` | Métricas | Resumen precalculado por espacio que expone volumen de tickets, campos en pantalla, campos homologados y estado de sincronización para consulta inmediata. |

---

## 5. Estrategia de Homologación de Campos Personalizados

Dado que cada espacio de Jira puede configurar formularios y campos de manera independiente, el sistema aplica una estrategia automatizada de homologación:

```
[ Esquema de Pantalla en Jira ] ---> [ Detección Automática ] ---> [ Regla de Homologación ] ---> [ Columna en Datamart ]
(ej. "Área Solicitante")              (excluye mensajes/core)       (cf_area_solicitante)           (vw_mart_<proyecto>)
```

### 5.1. Criterios de Selección y Filtrado
* **Detección por Pantallas:** El motor consulta la jerarquía de esquemas de pantalla asignados a los tipos de incidencia de cada proyecto. Únicamente los campos habilitados visualmente en Jira se consideran candidatos a homologación.
* **Exclusión de Campos Redundantes:** Se descartan de forma automática los campos del core estándar (que ya pertenecen a la tabla universal) y campos informativos de solo lectura de tipo mensaje corporativo.

### 5.2. Estrategias de Normalización

* **Campos de Texto y Selección Simple (`STRING`):** Se limpian espacios en blanco y se preserva el valor textual seleccionado o ingresado.
* **Campos Numéricos (`NUMBER`):** Se valida el formato y se almacena en columna de alta precisión para operaciones cuantitativas en Power BI.
* **Campos Temporales (`DATE`):** Se normalizan hacia marcas de tiempo con zona horaria estandarizada (`America/Lima`).
* **Selección en Cascada (`CASCADE_SPLIT`):** Aquellos campos con estructura jerárquica (padre e hijo) se subdividen automáticamente en dos atributos analíticos independientes: `cf_<campo>_parent` y `cf_<campo>_child`.
* **Selectores de Usuario (`USER_DIM`):** El usuario se incorpora a la dimensión de usuarios del Data Warehouse y en el hecho analítico se almacena tanto su nombre para mostrar como su identificador de cuenta para cruces analíticos.

---

## 6. Portal de Gestión y Administración Operativa

La plataforma dispone de una consola web diseñada para el equipo de Gobierno TI y soporte operativo, accesible internamente y expuesta de forma segura para reuniones de seguimiento diario (*dailys*).

### 6.1. Capacidades Funcionales del Portal

1. **Panel General de Sincronización:**
   * Indicadores clave de rendimiento: estado general del servicio, cantidad de espacios activos, volumen total de tickets en almacén y total de eventos de auditoría registrados.
   * **Monitor de Hilos en Tiempo Real:** Visualización en vivo del estado de los productores de extracción de Jira Cloud y del hilo consumidor de base de datos, reportando lotes en cola, volumen extraído y marcas de tiempo de actividad.
   * **Tabla Maestra de Proyectos:** Matriz de control con filtros por estado y buscador por código/nombre. Expone de manera diferenciada los **Campos en Pantalla** detectados en Jira frente a los **Campos Homologados** incorporados al Data Warehouse.
   * Control de proyectos para activar o pausar la sincronización programada individual o masivamente.
2. **Homologación de Campos:**
   * **Acciones Masivas en Un Clic:** Botones de auto-mapeo para generar de forma inmediata las reglas base de todos los espacios activos, y regularización secuencial del histórico sin requerir scripts manuales.
   * **Visor Tabular de Reglas:** Módulo de auditoría que permite filtrar por proyecto las reglas activas, verificando nombres analíticos de columna, estrategias y presencia en Datamart.
   * **Mapeo Manual Avanzado:** Formulario individual para incorporar casos atípicos o forzar comportamientos específicos.
3. **Explorador del Modelo de Datos (ER):**
   * Consulta instantánea de volumen de registros en vivo por cada tabla física.
   * Diagrama relacional interactivo de la arquitectura de base de datos.
4. **Mantenimiento y Auditoría:**
   * Diagnóstico de marcas de agua por proyecto, tiempos de ejecución y registro de bitácoras operativas.

---

## 7. Infraestructura, Despliegue y Resiliencia

El ecosistema opera mediante una arquitectura de microservicios contenerizados sobre Docker, optimizada para servidores con recursos acotados (mínimo 4 GB RAM, recomendado 8 GB RAM).

### 7.1. Matriz de Servicios y Distribución de Recursos

| Servicio | Rol Arquitectónico | Política de Recursos |
| :--- | :--- | :--- |
| `postgres-dw` | Motor de Data Warehouse Analítico (PostgreSQL 16). | Memoria compartida asignada en 512 MB para acelerar operaciones de agregación y joins analíticos sobre JSONB. |
| `postgres-airflow` | Almacén de metadatos internos del planificador. | Consumo ligero restringido a la gestión de estados de tareas. |
| `airflow-scheduler` | Motor planificador y ejecutor de pipelines (`LocalExecutor`). | Procesamiento multi-hilo acotado para evitar sobrecarga de CPU. |
| `airflow-webserver` | Servidor web y API REST de Airflow. | Interfaz de supervisión técnica de flujos de trabajo. |
| `etl-admin-ui` | Portal web de soporte y seguimiento operativo (Streamlit). | Arquitectura ligera con caché transaccional de 15 segundos y bajo consumo de memoria (<100 MB). |
| `cadvisor` | Recolector de telemetría de rendimiento de contenedores. | Supervisión de uso de CPU y memoria de la infraestructura. |
| `ngrok-tunnel` | Punto de enlace seguro para acceso externo a la UI. | Túnel HTTPS autenticado con credenciales básicas para acceso remoto. |

### 7.2. Mecanismos de Resiliencia y Seguridad

* **Aislamiento en Redes Privadas:** Toda la comunicación entre los componentes de base de datos, orquestación y portal transcurre en una red virtual interna de Docker, exponiendo hacia el exterior únicamente los puertos estrictamente necesarios.
* **Seguridad de Acceso:** La exposición del portal hacia redes externas mediante el túnel seguro requiere autenticación obligatoria y encriptación TLS de extremo a extremo.
* **Protección ante Fallos de Conexión:** Las consultas hacia Jira Cloud implementan políticas de reintento automático con retroceso exponencial (*exponential backoff*) y captura estructurada de códigos HTTP 429 (límite de tasa) y 404/403 (permisos restringidos).
* **Alertas Operativas:** El sistema cuenta con mecanismos de notificación inmediata mediante webhooks para alertar al equipo ante excepciones no controladas en cualquiera de los pipelines.

---

## 8. Guía Operativa de Mantenimiento

### 8.1. Periodicidad Recomendada de Procesos

* **Extracción Incremental:** Ejecución automática cada 15 a 30 minutos a través del orquestador Airflow para mantener la información analítica actualizada con respecto a la operativa diaria.
* **Conciliación de Eliminaciones:** Ejecución diaria nocturna o fuera de horario pico para contrastar el inventario de identificadores y asentar bajas de registros.
* **Auto-Mapeo de Nuevos Proyectos:** Ejecutable bajo demanda desde el portal web al crear o reconfigurar espacios de trabajo en Jira Cloud.
* **Mantenimiento de Base de Datos:** Ejecución semanal de estadísticas analíticas (`VACUUM ANALYZE`) para optimizar los planes de ejecución del motor PostgreSQL.

### 8.2. Diagnóstico Rápido de Estado

Para verificar la continuidad operativa del servicio, el equipo de soporte debe validar:
1. **Estado del Almacén:** Conectividad al puerto del Data Warehouse y estabilidad de volumen de registros en la vista general.
2. **Monitoreo de Hilos:** Ausencia de hilos bloqueados en el panel de telemetría en tiempo real del portal web.
3. **Marcas de Agua:** Que la diferencia de tiempo entre la última sincronización exitosa y la hora actual se mantenga dentro de la ventana de ejecución programada.

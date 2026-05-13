# BotFinanzas

Bot de Telegram para el registro y seguimiento de finanzas personales, usando Google Sheets como base de datos.

## Funcionalidades

- Registro de **ingresos**, **egresos** y **movimientos** entre cuentas
- Gestión de **deudas a plazos** con seguimiento de pagos
- Consulta de **saldos** por cuenta en tiempo real
- Cálculo de **patrimonio neto** (liquidez, ahorro, préstamos e inversiones)
- **Resúmenes automáticos** semanales y de fin de mes vía Telegram
- Catálogos configurables por usuario desde la hoja de Google Sheets
- Soporte multi-usuario (cada usuario con su propia hoja)

## Comandos

| Comando | Descripción |
|---|---|
| `/nuevo` o `/start` | Registrar ingreso, egreso o movimiento |
| `/nueva_deuda` | Registrar una nueva deuda a plazos |
| `/pagar` | Registrar pago de cuota de deuda activa |
| `/saldos` | Ver saldos actuales por cuenta |
| `/resumen` | Resumen de ingresos y egresos del mes actual |
| `/networth` | Desglose de patrimonio (liquidez, ahorro, inversiones) |
| `/neto` | Patrimonio bruto menos pasivos (deudas activas) |
| `/deudas` | Ver todas las deudas registradas |
| `/deudas_activas` | Ver solo las deudas con pagos pendientes |
| `/cancelar` | Cancelar la operación en curso |
| `/whoami` | Ver tu Telegram user ID |

## Tipos de movimiento

- **Normal**: transferencia entre dos cuentas líquidas
- **Ahorro**: mover fondos hacia o desde la cuenta de ahorro patrimonial
- **Inversión**: aportar o retirar de cuentas de inversión (en USD)
- **Préstamo**: registrar dinero dado o cobrado de una persona

## Requisitos previos

- Python 3.11+
- Un bot de Telegram (obtenido desde [@BotFather](https://t.me/BotFather))
- Una Google Service Account con acceso a la API de Google Sheets
- Una hoja de Google Sheets por usuario, compartida con el service account

### Estructura de la hoja de Google Sheets

La hoja debe contener las siguientes pestañas:

| Pestaña | Columnas |
|---|---|
| `Ingresos` | FECHA, FUENTE, CATEGORÍA, MONTO, MÉTODO, BANCO, NOTA |
| `Egresos` | FECHA, CATEGORÍA, MONTO, MÉTODO, BANCO, NOTA |
| `Movimientos` | FECHA, BOLSA_REMITENTE, REMITENTE, BOLSA_DESTINO, DESTINO, PERSONA_PRESTAMO, MONTO, MONTO_DESTINO, NOTA |
| `Deudas` | NOMBRE, A QUIÉN LE DEBO, FECHA DE PAGO, CUOTA, MESES, PAGADOS, PENDIENTES, SALDO, ESTADO |
| `Categorías` | FUENTES_ING, CATEG_ING, METODOS, BANCOS, CATEG_EGR, CUENTAS, PERSONAS_PRESTAMO |

## Instalación

```bash
git clone https://github.com/G-Gamboa/BotFinanzas.git
cd BotFinanzas
pip install -r requirements.txt
```

## Configuración

Crea las siguientes variables de entorno antes de ejecutar:

| Variable | Descripción |
|---|---|
| `BOT_TOKEN` | Token del bot de Telegram |
| `USER_SHEETS` | JSON con el mapeo `{"telegram_user_id": "google_sheet_id"}` |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | JSON completo de credenciales del service account de Google |

Ejemplo de `USER_SHEETS`:
```json
{"123456789": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms"}
```

### Obtener credenciales de Google

1. Crear un proyecto en [Google Cloud Console](https://console.cloud.google.com/)
2. Habilitar la API de **Google Sheets**
3. Crear una **Service Account** y descargar el JSON de credenciales
4. Compartir cada hoja de Google Sheets con el email del service account

## Ejecución

```bash
python main.py
```

Para producción (Heroku u otro PaaS), el `Procfile` incluido configura el proceso como worker:

```
worker: python main.py
```

## Estructura del proyecto

```
BotFinanzas/
├── main.py              # Arranque del bot y registro de handlers
├── config.py            # Variables de entorno y constantes
├── auth.py              # Control de acceso por user_id
├── helpers.py           # Parseo de fechas, montos y formato
├── catalogs.py          # Catálogos y segmentación de cuentas por rol
├── sheets_service.py    # Cliente de Google Sheets
├── sheet_utils.py       # Utilidades de lectura de hojas (headers/celdas)
├── finance.py           # Cálculos: resúmenes, saldos, networth y deudas
├── validators.py        # Validaciones del flujo de registro
├── renderers.py         # Generación de mensajes de resumen para Telegram
├── services.py          # Guardado en Sheets y ejecución de pagos de deuda
├── jobs.py              # Tareas programadas (resumen semanal y fin de mes)
├── keyboards.py         # Teclados inline de Telegram
├── state.py             # Gestión del estado de conversación por usuario
├── handlers/
│   ├── commands.py      # Handlers de comandos de Telegram
│   ├── conversation.py  # Callbacks de botones y entrada de texto libre
│   └── shared.py        # Carga de catálogos del usuario
├── requirements.txt
└── Procfile
```

## Personalización

Los catálogos (categorías, cuentas, bancos, fuentes de ingreso, personas de préstamo) se leen directamente desde la pestaña `Categorías` de la hoja del usuario. Basta con editar esa pestaña para personalizar las opciones sin tocar el código.

Los valores por defecto (usados si la hoja no tiene catálogos) se configuran en `config.py`:
- `FUENTES_ING`, `CATEG_ING`, `CATEG_EGR`: categorías de ingresos y egresos
- `METODOS`, `BANCOS`: métodos de pago y bancos disponibles
- `CUENTAS`: cuentas disponibles para movimientos
- `INV_CUENTAS_DEFAULT`: cuentas tratadas como inversión (en USD)
- `USD_TO_GTQ`: tasa de cambio para conversión en el patrimonio neto

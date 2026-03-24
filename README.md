# Bot Finanzas modular

Proyecto dividido a partir de la versión funcional actual.

## Estructura
- `main.py`: arranque y registro de handlers
- `config.py`: variables de entorno y constantes
- `helpers.py`: parseos, fechas y formato
- `catalogs.py`: catálogos y cuentas por rol
- `sheets_service.py`: conexión con Google Sheets
- `sheet_utils.py`: utilidades para leer encabezados/celdas
- `finance.py`: cálculos de resumen, saldos, networth y deudas
- `validators.py`: validaciones del flujo
- `renderers.py`: textos de resumen y salida
- `services.py`: guardado en Sheets y pago de deuda
- `jobs.py`: tareas programadas
- `handlers/commands.py`: comandos
- `handlers/conversation.py`: callbacks y entradas de texto
- `handlers/shared.py`: carga de catálogos del usuario

## Variables de entorno
- `BOT_TOKEN`
- `USER_SHEETS`
- `GOOGLE_SERVICE_ACCOUNT_JSON`

## Ejecución
```bash
pip install -r requirements.txt
python main.py
```

## Nota
Esta división busca mantener el mismo comportamiento de la versión funcional actual, pero con mejor orden para seguir creciendo.

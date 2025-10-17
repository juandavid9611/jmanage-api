import locale
import uvicorn
from mangum import Mangum
from fastapi import FastAPI
from api.tours import router as tours_router
from api.users import router as users_router
from fastapi.middleware.cors import CORSMiddleware
from api.payments import router as payments_router
from api.calendar import router as calendar_router
from api.scheduled import router as scheduled_router
from api.workspaces import router as workspaces_router
from api.friendly_scripts import router as friendly_scripts_router
from core.error_handlers import install_error_handlers
from core.logging_config import configure_logging
from core.request_context import RequestContextMiddleware
from utils.env_utils import _use_mangum

def create_app() -> FastAPI:
    configure_logging()
    app = FastAPI(title="SportsManagement API", version="7.0.0")

    origins = [
        "http://localhost",
        "http://localhost:3031",
        "http://localhost:3030"
    ]
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"]
    )
    app.add_middleware(RequestContextMiddleware)
    install_error_handlers(app)
    
    app.include_router(payments_router)
    app.include_router(calendar_router)
    app.include_router(tours_router)
    app.include_router(users_router)
    app.include_router(workspaces_router)
    app.include_router(friendly_scripts_router)
    app.include_router(scheduled_router)
    locale.setlocale(locale.LC_ALL, 'en_US.utf-8')
    return app

app = create_app()

if _use_mangum():
    handler = Mangum(app)

if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8085,
        reload=True,
        log_config=None,     # 🔹 Desactiva el logger por defecto de Uvicorn
        access_log=False,    # 🔹 Evita logs duplicados
    )
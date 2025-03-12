import logging
from fastapi import FastAPI

from app.api.routes import router
from app.core.config import get_settings
from genkg.app.utils.orientdb_helper import setup_schema_mapper

settings = get_settings()

app = FastAPI(
    title=settings.APP_NAME,
    debug=settings.DEBUG,
)

# Include our routes
app.include_router(router, prefix=settings.API_V1_STR)


# Startup event
@app.on_event("startup")
async def startup_event():
    settings.setup_logging()
    logging.getLogger("uvicorn.access").setLevel(logging.INFO)
    logging.debug("DEBUG logging enabled")
    logging.info(f"Starting up the {settings.APP_NAME} service")
    logging.info(f"Debug mode: {settings.DEBUG}")
    await setup_schema_mapper(settings)


# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    logging.info(f"Shutting down the {settings.APP_NAME} service")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=settings.DEBUG)

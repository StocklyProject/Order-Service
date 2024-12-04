from fastapi import FastAPI, APIRouter
from starlette.middleware.cors import CORSMiddleware
from . import routes as order_routes
app = FastAPI()

# 기존 코드 유지
router = APIRouter(prefix="/api/v1")
app.include_router(order_routes.router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
@app.get("/")
def hello():
    return {"message": "order service consumer 메인페이지입니다"}
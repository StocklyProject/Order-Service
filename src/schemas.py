from pydantic import BaseModel, Field
from typing import Optional

class DepositRequest(BaseModel):
    amount: int

class OrderRequest(BaseModel):
    order_type: str = Field(..., pattern="^(매수|매도)$", description="주문 유형: 매수 또는 매도")
    price_type: str = Field(..., pattern="^(지정가|시장가)$", description="가격 유형: 지정가 또는 시장가")
    symbol: str = Field(..., description="주문할 주식의 심볼")
    price: Optional[float] = Field(None, description="지정가 주문일 경우 가격 (시장가는 None)")
    quantity: int = Field(..., gt=0, description="주문 수량 (0보다 커야 함)")
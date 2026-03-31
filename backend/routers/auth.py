from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

MOCK_USERS = {
    "sl.coo@company.com": {
        "password": "password",
        "role": "SL_COO",
        "name": "Sarah Chen",
        "practice": "Technology",
    },
    "market.coo@company.com": {
        "password": "password",
        "role": "MARKET_COO",
        "name": "James Rodriguez",
        "bu": "Financial Services",
    },
    "cft.planner@company.com": {
        "password": "password",
        "role": "CFT_PLANNER",
        "name": "Priya Sharma",
    },
}


class LoginRequest(BaseModel):
    email: str
    password: str


class LoginResponse(BaseModel):
    token: str
    user: dict


@router.post("/login", response_model=LoginResponse)
def login(req: LoginRequest):
    user = MOCK_USERS.get(req.email)
    if not user or user["password"] != req.password:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    user_data = {k: v for k, v in user.items() if k != "password"}
    user_data["email"] = req.email
    return {
        "token": f"mock-token-{req.email}",
        "user": {"email": req.email, "role": user["role"], "name": user["name"]},
    }


@router.get("/me")
def get_me():
    return {"email": "sl.coo@company.com", "role": "SL_COO", "name": "Sarah Chen"}

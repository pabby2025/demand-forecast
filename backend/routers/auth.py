from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

router = APIRouter()

# In-memory user store. Admin sets access_code for a user; user sets password on first login.
# Key = email (lowercase)
MOCK_USERS: dict = {
    "prabhu@cognizant.com": {
        "access_code": "DFP-2026-PK",
        "password": "Cognizant@2026",   # pre-seeded demo password
        "name": "Prabhu K",
        "role": "SL_COO",
    },
    "admin@cognizant.com": {
        "access_code": "DFP-2026-ADM",
        "password": "Admin@2026",
        "name": "Platform Admin",
        "role": "SL_COO",
    },
    "demo.planner@cognizant.com": {
        "access_code": "DFP-2026-DP",
        "password": None,               # first-login only — must set password
        "name": "Demo Planner",
        "role": "CFT_PLANNER",
    },
}


class LoginRequest(BaseModel):
    email: str
    credential: str   # access_code OR password


class LoginResponse(BaseModel):
    token: str
    user: dict
    must_change_password: bool


class ChangePasswordRequest(BaseModel):
    email: str
    current_credential: str   # access_code or current password
    new_password: str


@router.post("/login", response_model=LoginResponse)
def login(req: LoginRequest):
    email = req.email.strip().lower()
    user = MOCK_USERS.get(email)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    credential = req.credential.strip()
    used_access_code = False

    # Check access code first, then password
    if credential == user["access_code"]:
        used_access_code = True
    elif user["password"] and credential == user["password"]:
        used_access_code = False
    else:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    must_change = used_access_code or (user["password"] is None)

    return {
        "token": f"mock-token-{email}",
        "user": {
            "email": email,
            "name": user["name"],
            "role": user["role"],
            "must_change_password": must_change,
        },
        "must_change_password": must_change,
    }


@router.post("/change-password")
def change_password(req: ChangePasswordRequest):
    email = req.email.strip().lower()
    user = MOCK_USERS.get(email)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    credential = req.current_credential.strip()
    # Validate: must match access_code or current password
    valid = (credential == user["access_code"]) or (
        user["password"] is not None and credential == user["password"]
    )
    if not valid:
        raise HTTPException(status_code=401, detail="Current credential is incorrect")

    if len(req.new_password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")

    # Update password in memory
    MOCK_USERS[email]["password"] = req.new_password

    return {"success": True, "message": "Password updated successfully"}


@router.get("/me")
def get_me():
    return {
        "email": "prabhu@cognizant.com",
        "role": "SL_COO",
        "name": "Prabhu K",
        "must_change_password": False,
    }

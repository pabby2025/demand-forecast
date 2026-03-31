from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import mock_data
import copy

router = APIRouter()

_feedback_store: list[dict] = copy.deepcopy(mock_data.get_feedback())


class FeedbackCreate(BaseModel):
    month: str
    cluster: str
    system_forecast: int
    mgmt_adjustment: int
    reason: str


class FeedbackSubmitRequest(BaseModel):
    scenario_inputs: list = []
    summary: dict = {}
    skill_updates: list = []
    feedback_text: str = ""
    action: str = "submit"  # "submit" | "audit_report"


class FeedbackUpdate(BaseModel):
    mgmt_adjustment: Optional[int] = None
    reason: Optional[str] = None
    status: Optional[str] = None


@router.post("/submit")
def submit_feedback(req: FeedbackSubmitRequest):
    """POST /api/v1/feedback/submit — management adjustment submission."""
    import uuid
    feedback_id = f"FB-{str(uuid.uuid4())[:8].upper()}"
    return {"success": True, "feedback_id": feedback_id}


@router.get("")
def list_feedback():
    return {"feedback": _feedback_store, "total": len(_feedback_store)}


@router.get("/{feedback_id}")
def get_feedback_item(feedback_id: str):
    item = next((f for f in _feedback_store if f["id"] == feedback_id), None)
    if not item:
        raise HTTPException(status_code=404, detail="Feedback not found")
    return item


@router.post("")
def create_feedback(fb: FeedbackCreate):
    new_id = f"FB-{400 + len(_feedback_store)}"
    new_item = {
        "id": new_id,
        "month": fb.month,
        "cluster": fb.cluster,
        "system_forecast": fb.system_forecast,
        "mgmt_adjustment": fb.mgmt_adjustment,
        "final_forecast": fb.mgmt_adjustment,
        "reason": fb.reason,
        "status": "Pending",
        "submitted_by": "Sarah Chen",
        "submitted_at": "2026-03-19",
    }
    _feedback_store.append(new_item)
    return new_item


@router.put("/{feedback_id}")
def update_feedback(feedback_id: str, update: FeedbackUpdate):
    item = next((f for f in _feedback_store if f["id"] == feedback_id), None)
    if not item:
        raise HTTPException(status_code=404, detail="Feedback not found")
    if update.mgmt_adjustment is not None:
        item["mgmt_adjustment"] = update.mgmt_adjustment
        item["final_forecast"] = update.mgmt_adjustment
    if update.reason is not None:
        item["reason"] = update.reason
    if update.status is not None:
        item["status"] = update.status
    return item


@router.delete("/{feedback_id}")
def delete_feedback(feedback_id: str):
    global _feedback_store
    item = next((f for f in _feedback_store if f["id"] == feedback_id), None)
    if not item:
        raise HTTPException(status_code=404, detail="Feedback not found")
    _feedback_store = [f for f in _feedback_store if f["id"] != feedback_id]
    return {"message": "Feedback deleted"}

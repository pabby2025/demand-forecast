from fastapi import APIRouter, HTTPException
from typing import Optional
import mock_data
import copy

router = APIRouter()

_alert_store: list[dict] = copy.deepcopy(mock_data.get_alerts())


@router.get("")
def list_alerts(status: Optional[str] = None):
    alerts = _alert_store
    if status:
        alerts = [a for a in alerts if a["status"].lower() == status.lower()]
    unread = sum(1 for a in _alert_store if a["status"] == "New")
    return {"alerts": alerts, "total": len(alerts), "unread": unread}


@router.put("/{alert_id}/acknowledge")
def acknowledge_alert(alert_id: str):
    alert = next((a for a in _alert_store if a["id"] == alert_id), None)
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    alert["status"] = "Acknowledged"
    return alert


@router.put("/{alert_id}/dismiss")
def dismiss_alert(alert_id: str):
    alert = next((a for a in _alert_store if a["id"] == alert_id), None)
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    alert["status"] = "Dismissed"
    return alert


@router.delete("/{alert_id}")
def delete_alert(alert_id: str):
    global _alert_store
    alert = next((a for a in _alert_store if a["id"] == alert_id), None)
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    _alert_store = [a for a in _alert_store if a["id"] != alert_id]
    return {"message": "Alert deleted"}

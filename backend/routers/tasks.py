from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import mock_data
import copy

router = APIRouter()

# In-memory store initialized from mock data
_task_store: list[dict] = copy.deepcopy(mock_data.get_tasks())


class TaskCreate(BaseModel):
    title: str
    type: str
    priority: str
    due_date: str
    assigned_by: str
    description: Optional[str] = ""
    cluster: Optional[str] = ""


class TaskUpdate(BaseModel):
    title: Optional[str] = None
    type: Optional[str] = None
    priority: Optional[str] = None
    due_date: Optional[str] = None
    status: Optional[str] = None
    description: Optional[str] = None


@router.get("")
def list_tasks(status: Optional[str] = None):
    tasks = _task_store
    if status:
        tasks = [t for t in tasks if t["status"].lower() == status.lower()]
    return {"tasks": tasks, "total": len(tasks)}


@router.get("/{task_id}")
def get_task(task_id: str):
    task = next((t for t in _task_store if t["id"] == task_id), None)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@router.post("")
def create_task(task: TaskCreate):
    new_id = f"TASK-{200 + len(_task_store)}"
    new_task = {
        "id": new_id,
        "title": task.title,
        "type": task.type,
        "priority": task.priority,
        "due_date": task.due_date,
        "assigned_by": task.assigned_by,
        "status": "Pending",
        "description": task.description,
        "cluster": task.cluster,
    }
    _task_store.append(new_task)
    return new_task


@router.put("/{task_id}")
def update_task(task_id: str, update: TaskUpdate):
    task = next((t for t in _task_store if t["id"] == task_id), None)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    for field, value in update.model_dump(exclude_none=True).items():
        task[field] = value
    return task


@router.delete("/{task_id}")
def delete_task(task_id: str):
    global _task_store
    task = next((t for t in _task_store if t["id"] == task_id), None)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    _task_store = [t for t in _task_store if t["id"] != task_id]
    return {"message": "Task deleted"}

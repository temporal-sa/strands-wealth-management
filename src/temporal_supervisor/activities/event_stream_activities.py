from temporalio import activity

from common.event_stream_manager import EventStreamManager
from common.user_message import ChatInteraction
from common.status_update import StatusUpdate


class EventStreamActivities:
    """Persists conversation history and status updates to Redis."""

    @staticmethod
    @activity.defn
    async def append_chat_interaction(workflow_id: str, chat_interaction: ChatInteraction) -> int:
        manager = EventStreamManager()
        try:
            return await manager.append_chat_interaction(workflow_id, chat_interaction)
        finally:
            await manager.close()

    @staticmethod
    @activity.defn
    async def append_status_update(workflow_id: str, status_update: StatusUpdate) -> int:
        manager = EventStreamManager()
        try:
            return await manager.append_status_update(workflow_id, status_update)
        finally:
            await manager.close()

    @staticmethod
    @activity.defn
    async def delete_conversation(workflow_id: str) -> bool:
        manager = EventStreamManager()
        try:
            return await manager.delete_stream(workflow_id)
        finally:
            await manager.close()

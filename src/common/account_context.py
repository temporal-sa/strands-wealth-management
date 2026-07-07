from pydantic import BaseModel


class UpdateAccountOpeningStateInput(BaseModel):
    account_name: str
    state: str
    workflow_id: str = ""

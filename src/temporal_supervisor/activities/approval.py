from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from common.approval_prompts import friendly_confirmation


class ApprovalActivities:
    """Resolves human-approval prompts off the workflow.

    The approval hook runs in workflow context and must stay deterministic, so the
    name lookup (which reads the data files) is done here as an activity.
    """

    @staticmethod
    @activity.defn
    async def describe_approval(tool_name: str, tool_input: dict) -> str:
        """Return a friendly confirmation prompt for a destructive tool call."""
        activity.logger.info(f"describe_approval: {tool_name} {tool_input}")
        return friendly_confirmation(tool_name, tool_input) or f"Approve {tool_name} with {tool_input}?"
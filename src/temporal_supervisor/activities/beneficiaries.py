from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from common.beneficiaries_manager import BeneficiariesManager


class Beneficiaries:
    """Beneficiary operations, exposed to the agent via ``activity_as_tool``.

    Flat primitive arguments keep the generated tool schema simple for the model.
    """

    @staticmethod
    @activity.defn
    async def list_beneficiaries(client_id: str) -> list:
        """List all beneficiaries for a given client."""
        activity.logger.info(f"list_beneficiaries: {client_id}")
        return BeneficiariesManager().list_beneficiaries(client_id)

    @staticmethod
    @activity.defn
    async def add_beneficiary(client_id: str, first_name: str, last_name: str, relationship: str) -> str:
        """Add a new beneficiary for a client."""
        activity.logger.info(f"add_beneficiary: {client_id}, {first_name} {last_name} ({relationship})")
        BeneficiariesManager().add_beneficiary(client_id, first_name, last_name, relationship)
        return f"Beneficiary {first_name} {last_name} added successfully."

    @staticmethod
    @activity.defn
    async def delete_beneficiary(client_id: str, beneficiary_id: str) -> str:
        """Delete a beneficiary for a client."""
        activity.logger.info(f"delete_beneficiary: {client_id}, {beneficiary_id}")
        manager = BeneficiariesManager()
        name = manager.display_name(client_id, beneficiary_id)
        manager.delete_beneficiary(client_id, beneficiary_id)
        return f"{name} deleted successfully." if name else f"Beneficiary {beneficiary_id} deleted successfully."

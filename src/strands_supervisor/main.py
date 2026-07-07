import logging
import os
import sys

# Add src/ to the path so `common` / `strands_supervisor` imports work when this
# file is run directly (e.g. `uv run python src/strands_supervisor/main.py`).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv

from strands_supervisor.agents import build_supervisor

load_dotenv()

# Strands logs tool/model activity at INFO; keep the CLI clean.
logging.getLogger("strands").setLevel(logging.WARNING)


def run_conversation():
    """Run an interactive CLI conversation with the wealth management agents."""
    supervisor = build_supervisor()

    print("=" * 60)
    print("  Wealth Management Assistant (Strands)")
    print("=" * 60)
    print("Type your message and press Enter. Type 'exit', 'quit', or 'end' to stop.\n")

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not user_input:
            continue

        if user_input.lower() in ("exit", "quit", "end"):
            print("Goodbye!")
            break

        try:
            result = supervisor(user_input)
            response_text = str(result).strip()
        except Exception as e:
            response_text = f"[Error: {e}]"

        print(f"\nAssistant: {response_text}\n")


def main():
    run_conversation()


if __name__ == "__main__":
    main()

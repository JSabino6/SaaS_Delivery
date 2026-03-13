"""
Snippet de portfolio: estrategia de regressao para agente conversacional.

Demonstra como validar cenarios de negocio e comportamento esperado sem expor
regras proprietarias completas.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List


@dataclass
class Scenario:
    name: str
    input_text: str
    expected_action: str
    expected_contains: str


class FakeAgent:
    """Stub simplificado para demonstrar o harness de regressao."""

    def run(self, text: str) -> Dict[str, str]:
        lowered = text.lower()
        if "adicionar" in lowered or "quero" in lowered:
            return {"action": "add_item", "message": "Item adicionado ao carrinho."}
        if "retirar" in lowered or "remover" in lowered:
            return {"action": "remove_item", "message": "Item removido do carrinho."}
        if "pagar" in lowered or "checkout" in lowered:
            return {"action": "confirm_order", "message": "Vamos para confirmacao."} 
        return {"action": "ask_clarification", "message": "Pode detalhar seu pedido?"}


def run_regression_suite(
    agent_factory: Callable[[], FakeAgent],
    scenarios: List[Scenario],
) -> Dict[str, int]:
    total = len(scenarios)
    passed = 0

    for case in scenarios:
        agent = agent_factory()
        result = agent.run(case.input_text)

        action_ok = result.get("action") == case.expected_action
        message_ok = case.expected_contains.lower() in result.get("message", "").lower()

        if action_ok and message_ok:
            passed += 1
        else:
            print(f"[FAIL] {case.name}")
            print(f"  expected action: {case.expected_action}")
            print(f"  actual action:   {result.get('action')}")
            print(f"  expected text:   {case.expected_contains}")
            print(f"  actual text:     {result.get('message')}")

    return {
        "total": total,
        "passed": passed,
        "failed": total - passed,
    }


def default_scenarios() -> List[Scenario]:
    return [
        Scenario(
            name="add item intent",
            input_text="Quero adicionar uma pizza calabresa",
            expected_action="add_item",
            expected_contains="adicionado",
        ),
        Scenario(
            name="remove item intent",
            input_text="Pode remover o refrigerante",
            expected_action="remove_item",
            expected_contains="removido",
        ),
        Scenario(
            name="checkout intent",
            input_text="Vamos pagar e fechar o pedido",
            expected_action="confirm_order",
            expected_contains="confirmacao",
        ),
    ]


if __name__ == "__main__":
    report = run_regression_suite(FakeAgent, default_scenarios())
    print(report)

"""
Snippet de portfolio: maquina de estados simplificada para checkout.

Mostra o padrao de transicao deterministica usado para reduzir inconsistencias
entre conversa, carrinho e confirmacao final.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class CheckoutState(str, Enum):
    BROWSING = "browsing"
    CART_READY = "cart_ready"
    ADDRESS_CONFIRMED = "address_confirmed"
    PAYMENT_CONFIRMED = "payment_confirmed"
    COMPLETED = "completed"


@dataclass
class CheckoutSession:
    state: CheckoutState = CheckoutState.BROWSING

    def add_item(self) -> None:
        if self.state == CheckoutState.BROWSING:
            self.state = CheckoutState.CART_READY

    def confirm_address(self) -> None:
        if self.state != CheckoutState.CART_READY: 
            raise ValueError("Endereco so pode ser confirmado com carrinho pronto.")
        self.state = CheckoutState.ADDRESS_CONFIRMED

    def confirm_payment(self) -> None:
        if self.state != CheckoutState.ADDRESS_CONFIRMED:
            raise ValueError("Pagamento so pode ser confirmado apos endereco.")
        self.state = CheckoutState.PAYMENT_CONFIRMED

    def finalize(self) -> None:
        if self.state != CheckoutState.PAYMENT_CONFIRMED:
            raise ValueError("Pedido so pode finalizar apos pagamento confirmado.")
        self.state = CheckoutState.COMPLETED


if __name__ == "__main__":
    flow = CheckoutSession()
    flow.add_item()
    flow.confirm_address()
    flow.confirm_payment()
    flow.finalize()
    print({"checkout_state": flow.state.value})

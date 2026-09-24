"""Provider-neutral bootstrap core for Oriel."""

from .application.text_gateway import TextGateway, TurnResult

__all__ = ["TextGateway", "TurnResult"]

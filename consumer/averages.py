import math
from dataclasses import dataclass, field

from consumer.errors import InvalidOrder


def validate_order(order):
    if not isinstance(order, dict):
        raise InvalidOrder("Order must be a record, not a tombstone or scalar")
    for name in ("orderId", "product"):
        if not isinstance(order.get(name), str) or not order[name].strip():
            raise InvalidOrder(f"{name} must be a nonempty string")
    price = order.get("price")
    if isinstance(price, bool) or not isinstance(price, (int, float)):
        raise InvalidOrder("price must be numeric")
    if not math.isfinite(price) or price < 0:
        raise InvalidOrder("price must be finite and nonnegative")


@dataclass
class PriceStats:
    count: int = 0
    total: float = 0.0

    @property
    def average(self):
        return self.total / self.count if self.count else 0.0

    def add(self, price):
        self.count += 1
        self.total += price

    def snapshot(self):
        return {"count": self.count, "sum": self.total, "average": self.average}


@dataclass
class RunningAverages:
    global_stats: PriceStats = field(default_factory=PriceStats)
    products: dict[str, PriceStats] = field(default_factory=dict)

    def add(self, order):
        validate_order(order)
        self.global_stats.add(order["price"])
        self.products.setdefault(order["product"], PriceStats()).add(order["price"])
        return self.snapshot()

    def snapshot(self):
        return {
            "global": self.global_stats.snapshot(),
            "products": {
                product: stats.snapshot() for product, stats in sorted(self.products.items())
            },
        }
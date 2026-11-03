"""Deterministic generation of the four source entities.

Customers and products are "masters": a fixed population built once from the
seed, each carrying its own version history on a long timeline. That means any
batch date can be resolved without keeping state between runs -- the simulator
never reads yesterday's output to produce today's.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

from .chaos import (
    ChaosLog,
    decide_delay,
    decide_duplicates,
    maybe_break_total,
    maybe_negative_quantity,
)
from .config import SimulatorConfig, money, rng_for

# Masters live on this timeline so that any batch date resolves to a version.
EPOCH_START = date(2024, 1, 1)
EPOCH_END = date(2028, 12, 31)

# (country, region, city, postal_code) -- region is the SCD2 field, so the list
# is deliberately short enough that regional revenue has real group sizes.
LOCATIONS = [
    ("Nigeria", "Lagos", "Ikeja", "100001"),
    ("Nigeria", "Lagos", "Lekki", "106104"),
    ("Nigeria", "Abuja FCT", "Garki", "900104"),
    ("Nigeria", "Rivers", "Port Harcourt", "500272"),
    ("Nigeria", "Kano", "Nassarawa", "700213"),
    ("Nigeria", "Oyo", "Ibadan", "200223"),
    ("Ghana", "Greater Accra", "Accra", "GA184"),
    ("Ghana", "Ashanti", "Kumasi", "AK039"),
    ("Kenya", "Nairobi", "Westlands", "00100"),
    ("South Africa", "Gauteng", "Sandton", "2196"),
    ("South Africa", "Western Cape", "Claremont", "7708"),
]

STREETS = [
    "Allen Avenue", "Awolowo Road", "Adeola Odeku Street", "Herbert Macaulay Way",
    "Gimbiya Street", "Aba Road", "Zoo Road", "Ring Road", "Oxford Street",
    "Liberation Road", "Harley Street", "Waiyaki Way", "Rivonia Road", "Main Road",
]

FIRST_NAMES = [
    "Abdulaziz", "Chiamaka", "Emeka", "Folake", "Ibrahim", "Ngozi", "Yusuf", "Adaeze",
    "Tunde", "Halima", "Chinedu", "Aisha", "Segun", "Blessing", "Musa", "Temitope",
    "Kwame", "Akosua", "Yaw", "Abena", "Kofi", "Esi", "Wanjiru", "Kamau", "Achieng",
    "Otieno", "Thabo", "Lerato", "Sipho", "Nomvula", "Bongani", "Zanele", "Farid",
    "Rukayat", "Obinna", "Hauwa", "Damilola", "Ifeoma", "Suleiman", "Oluwaseun",
]

LAST_NAMES = [
    "Adeyemi", "Okafor", "Bello", "Nwosu", "Lawal", "Eze", "Abubakar", "Obi",
    "Oyelaran", "Sani", "Chukwu", "Mohammed", "Balogun", "Udeh", "Danjuma",
    "Afolabi", "Mensah", "Boateng", "Asante", "Owusu", "Darko", "Addo",
    "Mwangi", "Kariuki", "Ochieng", "Njoroge", "Dlamini", "Mokoena", "Ndlovu",
    "Khumalo", "Mahlangu", "Zuma", "Ibrahim", "Yakubu", "Onyeka", "Garba",
    "Olatunji", "Ezeani", "Usman", "Adebayo",
]

EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "mail.com"]

PHONE_PREFIX = {
    "Nigeria": "+234", "Ghana": "+233", "Kenya": "+254", "South Africa": "+27",
}

SEGMENTS = ["retail", "retail", "retail", "retail", "wholesale"]

# category -> subcategory -> (min price, max price) in NGN
CATALOGUE = {
    "Electronics": {
        "Phones": (45_000, 850_000),
        "Laptops": (180_000, 1_900_000),
        "Audio": (8_000, 220_000),
    },
    "Home": {
        "Kitchen": (3_500, 140_000),
        "Bedding": (6_000, 95_000),
        "Lighting": (2_500, 60_000),
    },
    "Fashion": {
        "Footwear": (7_000, 120_000),
        "Bags": (9_000, 180_000),
        "Apparel": (4_000, 75_000),
    },
    "Groceries": {
        "Beverages": (800, 15_000),
        "Snacks": (500, 9_000),
        "Staples": (1_200, 32_000),
    },
}

BRANDS = ["Aurora", "Kesari", "NorthBay", "Vireo", "Lumen", "Takoda", "Saffron", "Orbit"]

PRODUCT_NOUNS = {
    "Phones": ["Smartphone", "Feature Phone", "Phone Case", "Screen Protector"],
    "Laptops": ["Ultrabook", "Gaming Laptop", "Chromebook", "Docking Station"],
    "Audio": ["Earbuds", "Headphones", "Bluetooth Speaker", "Soundbar"],
    "Kitchen": ["Blender", "Kettle", "Cookware Set", "Air Fryer"],
    "Bedding": ["Duvet", "Pillow Set", "Bed Sheet", "Mattress Topper"],
    "Lighting": ["Desk Lamp", "LED Bulb", "Floor Lamp", "String Lights"],
    "Footwear": ["Sneakers", "Sandals", "Loafers", "Boots"],
    "Bags": ["Backpack", "Tote Bag", "Laptop Sleeve", "Duffel Bag"],
    "Apparel": ["T-Shirt", "Hoodie", "Jeans", "Jacket"],
    "Beverages": ["Fruit Juice", "Bottled Water", "Soft Drink", "Iced Tea"],
    "Snacks": ["Plantain Chips", "Biscuits", "Groundnuts", "Granola Bar"],
    "Staples": ["Rice", "Beans", "Cooking Oil", "Pasta"],
}

PAYMENT_METHODS = ["card", "card", "card", "transfer", "transfer", "cash_on_delivery"]

ORDER_STATUSES = [
    ("delivered", 0.62), ("shipped", 0.18), ("placed", 0.10),
    ("cancelled", 0.06), ("returned", 0.04),
]

# Monday .. Sunday. Gives dim_date day-of-week analysis something real to find.
WEEKDAY_FACTOR = [1.00, 0.97, 1.02, 1.08, 1.25, 1.18, 0.78]


# ---------------------------------------------------------------------------
# Masters
# ---------------------------------------------------------------------------

def _address_version(rng, effective_from: date, avoid_region: str | None = None) -> dict:
    """One address version. avoid_region forces a genuine regional change.

    Without it, a move can land in the same region, producing an SCD2 version
    whose region is unchanged -- realistic, but invisible in regional analysis.
    """
    choices = LOCATIONS
    if avoid_region is not None:
        alternatives = [loc for loc in LOCATIONS if loc[1] != avoid_region]
        if alternatives:
            choices = alternatives
    country, region, city, postal = choices[rng.randrange(len(choices))]
    return {
        "effective_from": effective_from,
        "address_line": f"{rng.randint(1, 240)} {rng.choice(STREETS)}",
        "city": city,
        "region": region,
        "country": country,
        "postal_code": postal,
        "customer_segment": rng.choice(SEGMENTS),
    }


def build_customer_master(cfg: SimulatorConfig) -> list[dict]:
    """The fixed customer population, each with its own address history."""
    customers: list[dict] = []
    for index in range(cfg.n_customers):
        customer_id = f"CUST-{index:05d}"
        rng = rng_for(cfg.master_seed, "customer", customer_id)

        signup = EPOCH_START + timedelta(days=rng.randint(0, 600))
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)

        versions = [_address_version(rng, signup)]
        if rng.random() < cfg.customer_mover_share:
            window_start = cfg.history_anchor - timedelta(days=cfg.history_days)
            move_count = rng.randint(1, cfg.max_moves_per_customer)
            move_dates = sorted(
                {
                    window_start + timedelta(days=rng.randint(1, max(1, cfg.history_days)))
                    for _ in range(move_count)
                }
            )
            for moved_on in move_dates:
                if moved_on <= signup or moved_on > EPOCH_END:
                    continue
                versions.append(
                    _address_version(rng, moved_on, avoid_region=versions[-1]["region"])
                )

        country = versions[0]["country"]
        customers.append(
            {
                "customer_id": customer_id,
                "first_name": first,
                "last_name": last,
                "email": f"{first}.{last}{index}@{rng.choice(EMAIL_DOMAINS)}".lower(),
                "phone": f"{PHONE_PREFIX[country]}{rng.randint(700000000, 909999999)}",
                "signup_date": signup,
                "versions": versions,
                # Missing contact details are a property of the customer, not of
                # the day, so the same record is blank in every snapshot.
                "email_missing": rng.random() < cfg.null_email_rate,
                "phone_missing": rng.random() < cfg.null_phone_rate,
            }
        )
    return customers


def build_product_master(cfg: SimulatorConfig) -> list[dict]:
    """The fixed product catalogue, with occasional repricing."""
    products: list[dict] = []
    categories = list(CATALOGUE.items())

    for index in range(cfg.n_products):
        product_id = f"PROD-{index:05d}"
        rng = rng_for(cfg.master_seed, "product", product_id)

        category, subcategories = categories[rng.randrange(len(categories))]
        subcategory, (low, high) = rng.choice(list(subcategories.items()))
        noun = rng.choice(PRODUCT_NOUNS[subcategory])
        brand = rng.choice(BRANDS)

        created = EPOCH_START + timedelta(days=rng.randint(0, 400))
        base_price = money(rng.uniform(low, high))

        prices = [{"effective_from": created, "list_price": base_price}]
        if rng.random() < cfg.product_repricing_share:
            changed_on = created + timedelta(days=rng.randint(180, 700))
            if changed_on <= EPOCH_END:
                factor = rng.choice([0.85, 0.92, 1.08, 1.15, 1.25])
                prices.append(
                    {"effective_from": changed_on, "list_price": money(base_price * factor)}
                )

        products.append(
            {
                "product_id": product_id,
                "product_name": f"{brand} {noun}",
                "category": category,
                "subcategory": subcategory,
                "brand": brand,
                "prices": prices,
                "margin_rate": round(rng.uniform(0.18, 0.45), 4),
                "is_active": rng.random() > 0.07,
                "created_at": datetime.combine(created, time(6, 0)),
            }
        )
    return products


def _version_as_of(versions: list[dict], as_of: date) -> dict:
    """The last version whose effective_from is on or before as_of."""
    current = versions[0]
    for version in versions:
        if version["effective_from"] <= as_of:
            current = version
        else:
            break
    return current


def customer_snapshot(customer: dict, as_of: date) -> dict | None:
    """One customer row as the source would report it on as_of.

    Returns None before the customer signed up, so the population grows over
    the history rather than appearing fully formed on day one.
    """
    if customer["signup_date"] > as_of:
        return None
    version = _version_as_of(customer["versions"], as_of)
    return {
        "customer_id": customer["customer_id"],
        "first_name": customer["first_name"],
        "last_name": customer["last_name"],
        "email": None if customer["email_missing"] else customer["email"],
        "phone": None if customer["phone_missing"] else customer["phone"],
        "address_line": version["address_line"],
        "city": version["city"],
        "region": version["region"],
        "country": version["country"],
        "postal_code": version["postal_code"],
        "signup_date": customer["signup_date"],
        "customer_segment": version["customer_segment"],
        # updated_at is when this version took effect. It is what dbt snapshot
        # keys change detection on, so it must move only when something did.
        "updated_at": datetime.combine(version["effective_from"], time(2, 0)),
    }


def product_snapshot(product: dict, as_of: date) -> dict:
    """One product row as the source would report it on as_of."""
    price = _version_as_of(product["prices"], as_of)
    list_price = price["list_price"]
    return {
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "subcategory": product["subcategory"],
        "brand": product["brand"],
        "list_price": list_price,
        "cost_price": money(list_price * (1 - product["margin_rate"])),
        "is_active": product["is_active"],
        "created_at": product["created_at"],
        "updated_at": datetime.combine(price["effective_from"], time(3, 0)),
    }


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

def _weighted_status(rng) -> str:
    roll = rng.random()
    cumulative = 0.0
    for status, weight in ORDER_STATUSES:
        cumulative += weight
        if roll < cumulative:
            return status
    return ORDER_STATUSES[0][0]


def orders_for_logical_date(
    cfg: SimulatorConfig,
    logical_date: date,
    customers: list[dict],
    products: list[dict],
    log: ChaosLog | None = None,
) -> list[dict]:
    """Every order whose business event date is logical_date.

    These are not necessarily the orders that appear in that day's file -- see
    collect_batch, which routes them by emission date instead.
    """
    day_rng = rng_for(cfg.master_seed, "day", logical_date.isoformat())
    base = day_rng.randint(cfg.orders_per_day_min, cfg.orders_per_day_max)
    order_count = max(1, round(base * WEEKDAY_FACTOR[logical_date.weekday()]))

    eligible = [c for c in customers if c["signup_date"] <= logical_date]
    if not eligible:
        return []

    orders: list[dict] = []
    for sequence in range(order_count):
        order_id = f"ORD-{logical_date:%Y%m%d}-{sequence:05d}"
        rng = rng_for(cfg.master_seed, "order", order_id)

        customer = eligible[rng.randrange(len(eligible))]
        placed_at = datetime.combine(logical_date, time(0, 0)) + timedelta(
            hours=rng.randint(7, 22), minutes=rng.randint(0, 59), seconds=rng.randint(0, 59)
        )

        items: list[dict] = []
        for line_number in range(1, rng.randint(1, cfg.max_lines_per_order) + 1):
            product = products[rng.randrange(len(products))]
            priced = product_snapshot(product, logical_date)

            quantity, flipped = maybe_negative_quantity(rng, cfg, rng.randint(1, 4))
            # Price charged can differ from list price; the fact table must keep
            # the charged price, not look it up later.
            unit_price = money(priced["list_price"] * rng.choice([1.0, 1.0, 1.0, 0.95, 0.90]))
            gross = unit_price * quantity
            discount = money(gross * rng.choice([0.0, 0.0, 0.0, 0.05, 0.10])) if quantity > 0 else 0.0

            item_id = f"{order_id}-{line_number}"
            items.append(
                {
                    "order_item_id": item_id,
                    "order_id": order_id,
                    "product_id": product["product_id"],
                    "line_number": line_number,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    # Cost of goods at the moment of sale. Carried on the line
                    # for the same reason as unit_price: dim_product is SCD1 and
                    # overwrites cost_price, so margin computed from the
                    # dimension would change retroactively on every repricing.
                    "unit_cost": priced["cost_price"],
                    "discount_amount": discount,
                    "line_total": money(gross - discount),
                }
            )
            if flipped and log is not None:
                log.record("negative_quantity", item_id)

        shipping = money(rng.choice([0.0, 1500.0, 2500.0, 4000.0]))
        true_total = sum(item["line_total"] for item in items) + shipping
        reported_total, broken = maybe_break_total(rng, cfg, true_total)

        delay = decide_delay(rng, cfg)
        dup_in_file, dup_next_day = decide_duplicates(rng, cfg)

        first_emit = logical_date + timedelta(days=delay)
        emit_dates = [first_emit]
        if dup_next_day:
            emit_dates.append(first_emit + timedelta(days=1))

        if log is not None:
            if delay:
                log.record("late_arrival", f"{order_id}:+{delay}d")
            if broken:
                log.record("broken_total", order_id)
            if dup_in_file:
                log.record("duplicate_in_file", order_id)
            if dup_next_day:
                log.record("duplicate_next_day", order_id)

        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "order_placed_at": placed_at,
                "order_status": _weighted_status(rng),
                "order_currency": "NGN",
                "shipping_cost": shipping,
                "order_total": reported_total,
                "payment_method": rng.choice(PAYMENT_METHODS),
                "loyalty_tier": rng.choice(["bronze", "silver", "gold", "platinum"]),
                "logical_date": logical_date,
                "emit_dates": emit_dates,
                "copies_in_file": 2 if dup_in_file else 1,
                "items": items,
            }
        )
    return orders


def collect_batch(
    cfg: SimulatorConfig,
    batch_date: date,
    customers: list[dict],
    products: list[dict],
    log: ChaosLog | None = None,
) -> list[tuple[dict, int]]:
    """Orders that appear in batch_date's file, as (order, occurrence) pairs.

    Occurrence 0 is the first time the order was sent, 1 is a re-send. The
    window reaches back max_late_days + 1 because a late order can also be
    re-emitted the day after it finally arrives.
    """
    collected: list[tuple[dict, int]] = []
    window = cfg.max_late_days + 1

    for days_back in range(window + 1):
        logical_date = batch_date - timedelta(days=days_back)
        # Only log chaos for orders whose event date is the batch date,
        # otherwise a defect would be counted once per day it is in range.
        day_log = log if logical_date == batch_date else None
        for order in orders_for_logical_date(cfg, logical_date, customers, products, day_log):
            for occurrence, emit_date in enumerate(order["emit_dates"]):
                if emit_date == batch_date:
                    collected.append((order, occurrence))

    collected.sort(key=lambda pair: (pair[0]["order_id"], pair[1]))
    return collected

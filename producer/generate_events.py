"""Gera eventos de financiamento em JSONL para testes locais ou futura entrada Kafka."""

import argparse
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

VEHICLES = (
    {
        "vehicle_type": "CAR",
        "vehicle_brand": "Toyota",
        "vehicle_model": "Corolla XEi",
        "vehicle_year": 2025,
        "segment": "premium",
    },
    {
        "vehicle_type": "MOTORCYCLE",
        "vehicle_brand": "Honda",
        "vehicle_model": "CG 160 Titan",
        "vehicle_year": 2026,
        "segment": "mass_market",
    },
    {
        "vehicle_type": "CAR",
        "vehicle_brand": "Volkswagen",
        "vehicle_model": "T-Cross Comfortline",
        "vehicle_year": 2024,
        "segment": "standard",
    },
    {
        "vehicle_type": "MOTORCYCLE",
        "vehicle_brand": "Yamaha",
        "vehicle_model": "Fazer FZ25",
        "vehicle_year": 2025,
        "segment": "mass_market",
    },
)

LOCATIONS = (
    {"region": "SOUTHEAST", "state": "SP", "city": "Sao Paulo"},
    {"region": "SOUTHEAST", "state": "SP", "city": "Campinas"},
    {"region": "SOUTH", "state": "PR", "city": "Curitiba"},
    {"region": "NORTHEAST", "state": "BA", "city": "Salvador"},
)

STATUSES = ("APPROVED", "ACTIVE", "PENDING_APPROVAL")


def generate_event(event_id: int) -> dict:
    """Cria um evento de financiamento de veículo."""

    vehicle = random.choice(VEHICLES)
    location = random.choice(LOCATIONS)

    financed_amount = round(random.uniform(15000, 150000), 2)
    down_payment = round(financed_amount * random.uniform(0.20, 0.35), 2)

    installment_count = random.choice((24, 36, 48, 60))
    interest_rate_monthly = round(random.uniform(1.29, 1.99), 2)

    # Pequena taxa de eventos inválidos para testar o TRUSTED.
    if random.random() < 0.05:
        financed_amount = -round(random.uniform(100, 5000), 2)

    monthly_installment = (
        round(financed_amount / installment_count * 1.25, 2)
        if installment_count
        else 0.0
    )

    return {
        "financing_id": f"FIN-{datetime.now(timezone.utc):%Y%m%d}-{event_id:06d}",
        "customer_id": f"CUS-{random.randrange(10000000, 99999999):08d}",
        "vehicle_type": vehicle["vehicle_type"],
        "vehicle_id": f"VEH-{random.randrange(10000000, 99999999):08d}",
        "vehicle_brand": vehicle["vehicle_brand"],
        "vehicle_model": vehicle["vehicle_model"],
        "vehicle_year": vehicle["vehicle_year"],
        "segment": vehicle["segment"],
        "region": location["region"],
        "state": location["state"],
        "city": location["city"],
        "financed_amount": financed_amount,
        "down_payment": down_payment,
        "installment_count": installment_count,
        "monthly_installment": monthly_installment,
        "interest_rate_monthly": interest_rate_monthly,
        "status": random.choice(STATUSES),
        "currency": "BRL",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "--events",
        type=int,
        default=20,
        help="Quantidade de eventos a serem gerados.",
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=0.0,
        help="Intervalo em segundos entre os eventos.",
    )

    parser.add_argument(
        "--output",
        default="data/input/financing_events.jsonl",
        help="Arquivo JSONL de saída.",
    )

    args = parser.parse_args()

    if args.events <= 0:
        parser.error("--events deve ser maior que zero.")

    if args.interval < 0:
        parser.error("--interval não pode ser negativo.")

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("w", encoding="utf-8") as file:
        for event_id in range(1, args.events + 1):
            event = generate_event(event_id)
            # Log the event to stdout for visibility
            print(json.dumps(event, ensure_ascii=False))

            file.write(json.dumps(event, ensure_ascii=False) + "\n")

            # Disponibiliza cada evento imediatamente para o consumidor.
            file.flush()

            if args.interval > 0:
                time.sleep(args.interval)

    print(f"{args.events} eventos escritos em {output}")


if __name__ == "__main__":
    main()

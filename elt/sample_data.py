"""Synthetic source fixtures for FreshCart Bronze ingestion and enrichment."""

from __future__ import annotations

WEB_ORDERS_SAMPLE: list[dict] = [
    {
        "order_id": "ORD-1001",
        "cust_email": " Priya.R@Gmail.com ",
        "cust_name": "priya r",
        "order_date": "2026-09-01",
        "amount": "1,250.00",
        "currency": "INR",
        "pincode": "600041",
        "updated_at": "2026-09-01 10:02:00",
    },
    {
        "order_id": "ORD-1003",
        "cust_email": "meera@outlook.com",
        "cust_name": "MEERA S",
        "order_date": "2026-09-02",
        "amount": None,
        "currency": "INR",
        "pincode": "400050",
        "updated_at": "2026-09-02 09:10:00",
    },
    {
        "order_id": "ORD-1002",
        "cust_email": "Arjun.K@Yahoo.in ",
        "cust_name": "Arjun K",
        "order_date": "2026-09-01",
        "amount": "42.50",
        "currency": "USD",
        "pincode": "560034",
        "updated_at": "2026-09-01 11:15:00",
    },
    {
        "order_id": "ORD-1004",
        "cust_email": "rahul.m@gmail.com",
        "cust_name": "Rahul M",
        "order_date": "Sep 3, 2026",
        "amount": "-500",
        "currency": "INR",
        "pincode": "110001",
        "updated_at": "2026-09-03 12:00:00",
    },
    {
        "order_id": "ORD-1005",
        "cust_email": "priya.r@gmail.com",
        "cust_name": "Priya R.",
        "order_date": "2026-09-03",
        "amount": "₹ 899",
        "currency": "inr",
        "pincode": "600041",
        "updated_at": "2026-09-03 16:45:00",
    },
    {
        "order_id": "ORD-1006",
        "cust_email": "vikram@@mail",
        "cust_name": "Vikram",
        "order_date": "2026-09-04",
        "amount": 30,
        "currency": "EUR",
        "pincode": "99999",
        "updated_at": "2026-09-04 08:30:00",
    },
]

MOBILE_ORDERS_SAMPLE: list[dict] = [
    {
        "orderId": "ORD-1002",
        "customer": {"email": "arjun.k@yahoo.in", "name": "Arjun K"},
        "orderDate": "01/09/2026",
        "total": {"value": "42.50", "currency": "usd"},
        "deliveryPin": "560034",
        "updatedAt": "2026-09-01T11:15:00",
    },
    {
        "orderId": "ORD-1003",
        "customer": {"email": "Meera@Outlook.com", "name": "Meera S"},
        "orderDate": "02/09/2026",
        "total": {"value": "2,100.00", "currency": "INR"},
        "deliveryPin": "400050",
        "updatedAt": "2026-09-02T18:40:00",
    },
]

CRM_TIER_EVENTS_BASE: list[dict] = [
    {"cust_id": "CUST-01", "email": "priya.r@gmail.com", "tier": "Silver", "effective_from": "2023-04-10"},
    {"cust_id": "CUST-01", "email": "priya.r@gmail.com", "tier": "Gold", "effective_from": "2026-09-03"},
    {"cust_id": "CUST-02", "email": "arjun.k@yahoo.in", "tier": "Silver", "effective_from": "2024-01-15"},
    {"cust_id": "CUST-03", "email": "meera@outlook.com", "tier": "Platinum", "effective_from": "2022-11-01"},
]

FX_RATE_RESPONSES: list[dict] = [
    {
        "amount": 1.0,
        "base": "USD",
        "start_date": "2026-09-01",
        "end_date": "2026-09-03",
        "rates": {
            "2026-09-01": {"INR": 83.20},
            "2026-09-02": {"INR": 83.35},
            "2026-09-03": {"INR": 83.10},
        },
    }
]

PINCODE_DIRECTORY: dict[str, tuple[str, str]] = {
    "600041": ("Chennai", "Tamil Nadu"),
    "560034": ("Bengaluru", "Karnataka"),
    "400050": ("Mumbai", "Maharashtra"),
}


def build_pincode_lookup_payload(pincode: str) -> list[dict]:
    """Simulate the India Post pincode REST payload for a given 6-digit code."""
    entry = PINCODE_DIRECTORY.get(pincode)
    if entry is None:
        return [{"Message": "No records found", "Status": "Error", "PostOffice": None}]

    city, state = entry
    return [
        {
            "Message": "Number of pincode(s) found:1",
            "Status": "Success",
            "PostOffice": [
                {
                    "Name": city,
                    "District": city,
                    "State": state,
                    "Country": "India",
                    "Pincode": pincode,
                }
            ],
        }
    ]


# Backwards-compatible aliases
MOCK_WEB_ORDERS = WEB_ORDERS_SAMPLE
MOCK_MOBILE_ORDERS = MOBILE_ORDERS_SAMPLE
MOCK_CRM_TIER_EVENTS_BASE = CRM_TIER_EVENTS_BASE
MOCK_FX_RESPONSES = FX_RATE_RESPONSES
MOCK_PINCODES = PINCODE_DIRECTORY
mock_pincode_response = build_pincode_lookup_payload

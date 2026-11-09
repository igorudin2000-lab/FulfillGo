INSERT INTO inventory (sku, available)
VALUES
    ('PHONE-001', 1000),
    ('CASE-001', 2000),
    ('CHARGER-001', 1500)
ON CONFLICT (sku) DO NOTHING;


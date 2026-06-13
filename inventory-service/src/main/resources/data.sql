-- Datos de prueba para la tabla inventory
INSERT INTO inventory (id, product_id, product_name, quantity, reserved, available, created_at, updated_at)
VALUES
  ('1', 'PROD-001', 'Laptop Lenovo ThinkPad', 50, 0, 50, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('2', 'PROD-002', 'Monitor Samsung 24"', 30, 0, 30, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('3', 'PROD-003', 'Teclado Mecánico Logitech', 100, 0, 100, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

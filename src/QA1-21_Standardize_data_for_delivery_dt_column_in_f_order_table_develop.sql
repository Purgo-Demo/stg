-- Creating test data for purgo_playground.f_order
INSERT INTO purgo_playground.f_order (
    order_nbr, order_type, delivery_dt, order_qty, sched_dt, 
    expected_shipped_dt, actual_shipped_dt, order_line_nbr, loc_tracker_id, 
    shipping_add, primary_qty, open_qty, shipped_qty, order_desc, 
    flag_return, flag_cancel, cancel_dt, cancel_qty, crt_dt, updt_dt
) VALUES
-- Happy path test data (valid scenarios)
('ORD1', 1, 20240310, 100, 20240305, 20240308, 20240311, '001', 'LOC1', '123 Elm St', 100, 10, 90, 'Order 1 description', 'N', 'N', 20240307, 10, '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000'),

-- Edge cases (boundary conditions)
('ORD2', 0, 20241231, 1, 20240101, 20241230, 20250101, '002', 'LOC2', '456 Oak St', 1, 0, 1, 'Order 2 edge', 'Y', 'N', 20240310, 0, '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000'),

-- Error cases (invalid inputs)
('ORD3', -1, 20240000, -100, 20249999, 2024NULL, NULL, '003', 'LOC3', '789 Pine St', -100, -10, -90, 'Order 3 error', NULL, NULL, NULL, NULL, '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000'),

-- NULL handling scenarios
(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000'),

-- Special characters and multi-byte characters
('ORD4', 1, 20240515, 200, 20240512, 20240513, 20240516, '004', 'LOC4', '101 Maple St', 200, 0, 200, 'Special chars: !@#$%^&*()_+', '✔', '✖', 20240514, 0, '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000'),

-- Additional variable test data
('ORD5', 2, 20240601, 500, 20240531, 20240604, 20240607, '005', 'LOC5', 'Other Address!', 400, 20, 380, 'Multi-byte: テスト', 'N', 'Y', 20240602, 120, '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000'),
('ORD6', 3, 20240715, 300, 20240710, 20240713, 20240714, '006', 'LOC6', '102 Birch St', 300, 0, 300, 'No special case', 'N', 'N', 20240711, 0, '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000'),
('ORD7', 4, 20240820, NULL, NULL, NULL, NULL, '007', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, NULL, '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000'),
('ORD8', 5, 20240101, 0, 20240102, 20240103, 20240104, '008', 'LOC8', '104 Cedar St', 0, 100, 0, 'Boundary case zero', 'Y', 'Y', 20240102, 0, '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000'),
('ORD9', 6, 20241212, 150, 20241211, 20241210, 20241209, '009', 'LOC9', '105 Walnut St', 150, 50, 100, 'Reverse dates', 'N', 'N', 20241210, 50, '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000'),
('ORD10', 7, NULL, 80, 20240404, NULL, 20240405, '010', 'LOC10', '106 Chestnut St', 80, 10, 70, 'Partial NULL', 'N', 'N', 20240405, 10, '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000');

-- The code above generates diverse test data for various scenarios in the f_order table.

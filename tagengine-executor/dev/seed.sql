CREATE NAMESPACE IF NOT EXISTS demo.dcp;
CREATE TABLE IF NOT EXISTS demo.dcp.customer_wide (
  customer_id BIGINT, name STRING, region STRING, status STRING, amount DOUBLE
) USING iceberg;
INSERT INTO demo.dcp.customer_wide VALUES
  (1, 'A', '华东', 'gold',   15000.0),
  (2, 'B', '华北', 'black',  2000.0),
  (3, 'C', '华南', 'gold',   500.0),
  (4, 'D', '华东', 'gold',   12000.0),
  (5, 'E', '西南', 'silver', 800.0),
  (6, 'F', '华北', 'black',  60000.0);

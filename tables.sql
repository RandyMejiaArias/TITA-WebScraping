CREATE TABLE predictions (
  id INT AUTO_INCREMENT PRIMARY KEY,
  product_id INT NOT NULL,
  timestamp DATETIME NOT NULL,
  real_price FLOAT DEFAULT NULL,
  predicted_price FLOAT NOT NULL,
  day INT NOT NULL,
  month INT NOT NULL,
  day_of_week INT NOT NULL,
  days_since_start INT NOT NULL,
  rating FLOAT NOT NULL,
  moving_avg_3 FLOAT NOT NULL,
  moving_avg_7 FLOAT NOT NULL
);

CREATE TABLE model_errors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    mae FLOAT NOT NULL,
    rmse FLOAT NOT NULL,
    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
A.     Market risk management
1.      docker-compose build
2.      docker-compose up zookeeper kafka kafka-cli namenode datanode spark-master spark-worker
3.      docker-compose up kafka-producer
4.      docker-compose up spark-jobs
5.      docker-compose up train-job
** Đọc data vào các topic daily_prices của từng model
docker-compose exec -it kafka-cli bash
kafka-console-producer --bootstrap-server kafka:9092 --topic daily_prices_GSPC/XOM/CVX/BP
Copy các data ở dưới.
6.      docker-compose up model-service-gspc model-service-xom model-service-cvx model-service-bp

B.     Fraud Detection
1.      docker-compose up fraud-inference (tao api predict)
2.      docker-compose up fraud-producer
3.      docker-compose up fraud-consumer


Test samples:

{"Date":"2025-11-10 01:33:41","Company":"GSPC","Open":181.23,"High":183.74,"Low":179.20,"Close":182.12,"Volume":105382000}
{"Date":"2025-11-10 01:34:41","Company":"GSPC","Open":402.10,"High":408.90,"Low":399.50,"Close":405.60,"Volume":41230000}
{"Date":"2025-11-10 01:35:41","Company":"GSPC","Open":189.80,"High":198.40,"Low":188.50,"Close":195.70,"Volume":78200000}


{"Date":"2025-11-10 01:36:41","Company":"CVX","Open":142.18,"High":144.94,"Low":141.12,"Close":143.50,"Volume":23000000}

{"Date":"2025-11-10 01:39:41","Company":"CVX","Open":143.18,"High":145.94,"Low":142.12,"Close":144.50,"Volume":24000000}
{"Date":"2025-11-10 01:45:41","Company":"CVX","Open":144.18,"High":146.94,"Low":143.12,"Close":145.50,"Volume":25000000}


{"Date":"2025-11-10 01:50:41","Company":"BP","Open":145.18,"High":147.94,"Low":144.12,"Close":146.50,"Volume":26000000}
{"Date":"2025-11-10 01:54:41","Company":"BP","Open":146.18,"High":148.94,"Low":145.12,"Close":147.50,"Volume":27000000}
{"Date":"2025-11-10 02:00:41","Company":"BP","Open":147.18,"High":149.94,"Low":146.12,"Close":148.50,"Volume":28000000}


{"Date":"2025-11-10 02:36:41","Company":"XOM","Open":148.18,"High":150.94,"Low":147.12,"Close":149.50,"Volume":29000000}
{"Date":"2025-11-10 02:39:41","Company":"XOM","Open":148.18,"High":150.94,"Low":147.12,"Close":149.50,"Volume":29800000}
{"Date":"2025-11-10 02:50:41","Company":"XOM","Open":148.18,"High":150.94,"Low":147.12,"Close":149.50,"Volume":29907000}

# Market Risk Management and Fraud Detection System
## Tên thành viên
Group: 6
1. 23020350 Nguyễn Đăng Dương
2. 2302056 Bùi Hải Đăng
3. 23020360 Trương Trọng Đức
# Mô tả dự án
## Url to our report: 

** LƯU Ý: CẬP NHẬT ĐƯỜNG DẪN TRỰC TIẾP CỦA THƯ MỤC DATA (nhớ giải nén 2 file train_transaction và test_transaction) TRÊN MÁY CỦA BẠN VÀO HOST_DATA_PATH TRONG FILE .ENV (BẮT BUỘC VỚI DOCKEROPERATOR) 

*-----------------------------Cách chạy hệ thống---------------------------------------*
- Với hệ điều hành linux: dùng file Makefile: 
    + make build => Build các images và Up các containers
    + make up => Up các containers (khi đã build image rồi)
    + make down => Xóa các containers.
   
- Với hệ điều hành window: dùng file run.bat:
    + .\run.bat build => Build các images và Up các containers
    + .\run.bat up => Up các containers (khi đã build image rồi)
    + .\run.bat down => Xóa các containers.
  
----------------------------------------------------------------------------------------*
      
** Vào localhost:8088 giao diện của Airflow để chạy 2 dag (USERNAME: xinchao, PASSWORD: xinchao): deploy_pipeline và training_pipeline. (Click vào hình tam giác ở cột Actions bên phải), có thể click vào từng dag để xem mọi thứ như: status (running/success/failed), graphs, logs, v.v của dag đó.
    Thứ tự chạy các dags: Khi dag training_pipeline chạy thành công -> chạy dag deploy_pipeline.

** Song song với việc trên: Vào localhost:8080 giao diện của Apeche Spark để kiểm tra các job đã được nhận và đang thực thi như save_to_hdfs, train_risk_model, model_serivce_gspc/xom/bp/cvx.

** Với lần đầu tiên chạy hệ thống, mở terminal trong visual code hoặc của docker desktop, chạy các lệnh dưới đây để truyền dữ liệu cho Kafka để deploy realtime inference.

    - Đọc data vào các topic daily_prices của từng model
        1. docker-compose exec -it kafka-cli bash
        2. kafka-topics --bootstrap-server kafka:9092 --list (xem các topic trong kafka (optional)) 
        3. kafka-console-producer --bootstrap-server kafka:9092 --topic daily_prices_GSPC/CVX/BP/XOM
        4. Copy các data ở dưới ứng với từng công ty. (copy từng hàng 1 nhé)

Test samples:

{"Date":"2025-11-10 01:33:41","Company":"GSPC","Open":181.23,"High":183.74,"Low":179.20,"Close":182.12,"Volume":105382000}
{"Date":"2025-11-10 01:34:41","Company":"GSPC","Open":402.10,"High":408.90,"Low":399.50,"Close":405.60,"Volume":41230000}
{"Date":"2025-11-10 01:35:41","Company":"GSPC","Open":189.80,"High":198.40,"Low":188.50,"Close":195.70,"Volume":78200000}

(Xong mỗi công ty thì Ctrl C để chuyển sang nhập data cho công ty khác)

{"Date":"2025-11-10 01:36:41","Company":"CVX","Open":142.18,"High":144.94,"Low":141.12,"Close":143.50,"Volume":23000000}
{"Date":"2025-11-10 01:39:41","Company":"CVX","Open":143.18,"High":145.94,"Low":142.12,"Close":144.50,"Volume":24000000}
{"Date":"2025-11-10 01:45:41","Company":"CVX","Open":144.18,"High":146.94,"Low":143.12,"Close":145.50,"Volume":25000000}


{"Date":"2025-11-10 01:50:41","Company":"BP","Open":145.18,"High":147.94,"Low":144.12,"Close":146.50,"Volume":26000000}
{"Date":"2025-11-10 01:54:41","Company":"BP","Open":146.18,"High":148.94,"Low":145.12,"Close":147.50,"Volume":27000000}
{"Date":"2025-11-10 02:00:41","Company":"BP","Open":147.18,"High":149.94,"Low":146.12,"Close":148.50,"Volume":28000000}


{"Date":"2025-11-10 02:36:41","Company":"XOM","Open":148.18,"High":150.94,"Low":147.12,"Close":149.50,"Volume":29000000}
{"Date":"2025-11-10 02:39:41","Company":"XOM","Open":148.18,"High":150.94,"Low":147.12,"Close":149.50,"Volume":29800000}
{"Date":"2025-11-10 02:50:41","Company":"XOM","Open":148.18,"High":150.94,"Low":147.12,"Close":149.50,"Volume":29907000}


** Sau đó ta có thể xem kết quả dự đoán trong log của container model-service của từng công ty và container fraud-consumer hoặc vào localhost:8501 UI của hệ thống.




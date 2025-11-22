# Market Risk Management and Fraud Detection System
## Tên thành viên
Group: 6
1. 23020350 Nguyễn Đăng Dương
2. 2302056 Bùi Hải Đăng
3. 23020360 Trương Trọng Đức
# Mô tả dự án
## Url to our report: 
**1. Chủ đề:**  
Dự án nhóm triển khai là về **quản lý rủi ro thị trường** và **phát hiện giao dịch gian lận**. Trong bối cảnh chuyển đổi số mạnh mẽ của ngành tài chính và ngân hàng, dữ liệu đã trở thành nguồn tài nguyên chiến lược giúp các tổ chức đưa ra quyết định nhanh chóng, chính xác và an toàn hơn. Đặc biệt, đối với các thị trường có tốc độ giao dịch lớn như tài chính toàn cầu, việc quản lý rủi ro thị trường và phát hiện gian lận đóng vai trò then chốt trong việc duy trì tính ổn định và uy tín của hệ thống ngân hàng. HSBC một trong những tập đoàn tài chính lớn nhất thế giới là ví dụ tiêu biểu về việc ứng dụng Big Data vào quản trị rủi ro và phòng chống gian lận. Với hàng triệu giao dịch phát sinh mỗi ngày trên toàn cầu, HSBC đối mặt với lượng dữ liệu khổng lồ đến từ: hệ thống giao dịch chứng khoán, hoạt động khách hàng, dữ liệu thị trường, tin tức tài chính và nhiều nguồn phi cấu trúc khác như mạng xã hội. Khối lượng và tốc độ dữ liệu này khiến các phương pháp phân tích truyền thống không còn đủ hiệu quả trong việc giám sát rủi ro theo thời gian thực. 

**2. Tổng quan về dự án:**  
Dự án này được thực hiện với mục tiêu mô phỏng lại kiến trúc Big Data phục vụ Market Risk Management và Fraud Detection, dựa trên các công nghệ phổ biến như:  
– Apache Kafka –> thu thập và truyền dữ liệu giao dịch theo thời gian thực.  
– Hadoop HDFS –> lưu trữ dữ liệu lớn và lịch sử.  
– Apache Spark / PySpark –> xử lý dữ liệu batch và streaming.  
– PySpark MLlib –> huấn luyện mô hình máy học phục vụ phát hiện gian lận.  
– Apache Airflow –> điều phối pipeline xử lý dữ liệu.  
– Redis –> lưu trữ kết quả phân tích tốc độ cao.  
– Docker –> triển khai hệ thống theo mô hình container.  
Việc mô phỏng kiến trúc này không chỉ giúp hiểu rõ cách các tập đoàn tài chính toàn cầu vận hành hệ thống phân tích Big Data, mà còn chứng minh khả năng ứng dụng các công nghệ mã nguồn mở để xây dựng giải pháp phân tích dữ liệu quy mô lớn, có thể mở rộng và vận hành hiệu quả.

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





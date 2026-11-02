# DEX Whale Sentinel

Theo dõi swap từ DexPaprika, phân loại giao dịch theo giá trị và tính CVD trong cửa sổ trượt. Backend dùng FastAPI; trình duyệt nhận cập nhật qua WebSocket.

![Terminal](docs/terminal_preview.png)

## Chạy

```sh
python -m venv .venv
# Activate the environment, then:
pip install -r requirements.txt
python -m pytest tests -v
python -m uvicorn src.main:app --host 127.0.0.1 --port 8000
```

Mở http://localhost:8000. `GET /api/status` trả sức khỏe từng pool: lần lấy thành công, lỗi gần nhất, số lỗi liên tiếp và tuổi dữ liệu. Trạng thái DEGRADED nghĩa là có nguồn lỗi, cũ hoặc chạm giới hạn lấy bù.

## Luồng dữ liệu

HTTP client thử lại lỗi mạng, 429 và 5xx tối đa ba lần, có thời gian chờ tăng dần. Mỗi lượt poll đọc tối đa năm trang, dừng khi gặp giao dịch đã thấy hoặc hết trang. Nếu chạm giới hạn, trạng thái ghi rõ lịch sử có thể thiếu. Không coi đây là bảo đảm thu đủ giao dịch khi pool quá bận hoặc API đổi trang trong lúc đọc.

Khử trùng lặp dùng khóa gồm chain, pool và ID giao dịch, lưu có giới hạn trong RAM. Khởi động lại sẽ mất bộ nhớ này; chưa có cam kết xử lý exactly-once bền vững.

`POST /api/webhook/test` chỉ tạo bản xem trước và trả `delivered: false`. Chức năng gửi Telegram/Discord chưa được triển khai.

## Kiểm thử

Tests kiểm tra khử trùng lặp, phân loại, CVD, lỗi nguồn, retry, phục hồi và giới hạn lấy bù. HTTP trong các test dùng mock; không gửi cảnh báo ra ngoài.

import json
import os
from kafka import KafkaConsumer
import psycopg2
from pymongo import MongoClient

# --- CẤU HÌNH ---
# Bạn nên sử dụng biến môi trường trong thực tế
KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "localhost:9092")

# Các topic cần lắng nghe (thay bằng tên topic của bạn)
# Ví dụ: nếu database.server.name trong config Debezium là "pg-server-1"
# và bạn có các bảng "orders", "order_items", "users"
TOPICS = ["dbserver1.public.orders"] 

POSTGRES_CONN_STRING = "host='localhost' dbname='dinhnt' user='postgres' password='dinhnt'"
MONGO_CONN_STRING = os.environ.get("MONGO_CONN_STRING", "mongodb://localhost:27017/")
MONGO_DB = "dinhnt"
MONGO_COLLECTION = "users"


# --- HÀM TRUY VẤN VÀ TRẢI PHẲNG DỮ LIỆU ---

def get_full_order_details(order_id, pg_cursor):
    """
    Truy vấn tất cả dữ liệu liên quan đến một order_id và trải phẳng nó.
    Đây là logic cốt lõi bạn cần tùy chỉnh theo schema của mình.
    """
    query = """
    SELECT
        o.order_id,
        o.order_date,
        o.status AS order_status,
        u.user_id,
        u.username,
        u.email,
        json_agg(
            json_build_object(
                'product_id', p.product_id,
                'product_name', p.product_name,
                'price', p.price,
                'quantity', oi.quantity
            )
        ) FILTER (WHERE p.product_id IS NOT NULL) AS items
    FROM orders o
    LEFT JOIN users u ON o.user_id = u.user_id
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    LEFT JOIN products p ON oi.product_id = p.product_id
    WHERE o.order_id = %s
    GROUP BY o.order_id, u.user_id;
    """
    try:
        pg_cursor.execute(query, (order_id,))
        result = pg_cursor.fetchone()

        print(f"Result {result}")

        if not result:
            return None

        # Chuyển đổi kết quả từ tuple sang dictionary
        columns = [desc[0] for desc in pg_cursor.description]
        flat_document = dict(zip(columns, result))
        
        # Đặt _id cho MongoDB để có thể upsert, và đảm bảo nó duy nhất
        flat_document['_id'] = flat_document['order_id']
        
        return flat_document
    except Exception as e:
        print(f"Error fetching data for order_id {order_id}: {e}")
        return None


# --- VÒNG LẶP CHÍNH CỦA CONSUMER ---

def main():
    print("🚀 Starting Sync Service...")
    
    # Thiết lập kết nối
    try:
        pg_conn = psycopg2.connect(POSTGRES_CONN_STRING)
        mongo_client = MongoClient(MONGO_CONN_STRING)
        mongo_db = mongo_client[MONGO_DB]
        mongo_collection = mongo_db[MONGO_COLLECTION]
        print("✅ Connected to PostgreSQL and MongoDB.")

        # cursor = pg_conn.cursor()
        # cursor.execute("SELECT * FROM users LIMIT 5;") 
        # rows = cursor.fetchall()
        # print("Data", rows)
        
    except Exception as e:
        print(f"🔥 Could not connect to databases: {e}")
        return

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BROKER_URL,
        # Đọc message dưới dạng JSON
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        # Bắt đầu đọc từ đầu nếu là consumer mới
        auto_offset_reset='earliest',
        # Tự động commit offset
        enable_auto_commit=True,
        group_id='mongodb-sync-group' # Đặt tên group cho consumer
    )
    print(f"👂 Listening to consumer check: {consumer}")
    print(f"👂 Listening to Kafka topics: {', '.join(TOPICS)}")
    
    pg_cursor = pg_conn.cursor()

    for message in consumer:
        print("Topic:", consumer.subscription)
        try:
            payload = message.value.get("payload")
            if not payload:
                continue

            # Lấy thông tin về thao tác (create, update, delete)
            op = payload.get("op") 
            # Dữ liệu sau khi thay đổi (cho create/update)
            data = payload.get("after") if op != 'd' else payload.get("before")

            if not data:
                continue

            # Xác định ID chính cần được đồng bộ lại
            order_id = data.get("order_id")

            # Nếu sự kiện đến từ bảng `users`, chúng ta cần tìm tất cả các order liên quan
            if message.topic.endswith("users"):
                # Đây là phần xử lý nâng cao, tạm thời bỏ qua để đơn giản
                pass

            if not order_id:
                # print(f"⚠️ Message on topic {message.topic} doesn't have order_id. Skipping.")
                continue

            print(f"📬 Event (op: {op}) on topic '{message.topic}' for order_id: {order_id}. Rebuilding document...")

            if op == 'd' and message.topic.endswith("orders"):
                # Nếu toàn bộ đơn hàng bị xóa
                mongo_collection.delete_one({'_id': order_id})
                print(f"🗑️ Deleted document for order_id: {order_id} from MongoDB.")
            else:
                # Xây dựng lại toàn bộ document từ PostgreSQL
                full_document = get_full_order_details(order_id, pg_cursor)

                if full_document:
                    # Ghi đè (hoặc tạo mới) vào MongoDB
                    mongo_collection.update_one(
                        {'_id': full_document['_id']},
                        {'$set': full_document},
                        upsert=True
                    )
                    print(f"✅ Synced document for order_id: {order_id} to MongoDB.")

        except Exception as e:
            print(f"🔥 An error occurred while processing message: {e}")
            # Trong môi trường production, bạn cần có cơ chế xử lý lỗi tốt hơn
            # ví dụ như gửi message lỗi vào một Dead-Letter Queue (DLQ).
    
    pg_cursor.close()
    pg_conn.close()

if __name__ == "__main__":
    main()
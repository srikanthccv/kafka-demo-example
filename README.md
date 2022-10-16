### How to run

1. Start kafka 

```
docker-compose up -d
```
2. Start order service

```
cd order-service
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
BOOTSTRAP_SERVERS=localhost:9092 python3 app.py
```

3. Start shipment service

```
cd shipment-service
BOOTSTRAP_SERVERS=localhost:9092 go run app.go
```

4. Start invoice-service

```
cd invoice-service
BOOTSTRAP_SERVERS=localhost:9092 go run app.go
```

5. Start email-service

```
cd email-service
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
BOOTSTRAP_SERVERS=localhost:9092 python3 app.py
```

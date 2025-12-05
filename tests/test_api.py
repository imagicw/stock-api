from datetime import datetime, timedelta

def test_get_stock_info(client):
    # Test US stock
    response = client.get("/stock/AAPL")
    assert response.status_code == 200
    data = response.json()
    assert data["code"] == 0
    assert data["data"]["symbol"] == "AAPL"
    assert data["data"]["market"] == "US"
    assert "price" in data["data"]

    # Test CN stock (should normalize)
    response = client.get("/stock/sh600000")
    assert response.status_code == 200
    data = response.json()
    assert data["code"] == 0
    assert data["data"]["symbol"] == "sh600000" # Provider returns what was asked? Or normalized? 
    # In provider.py we returned 'symbol' as input. 
    # Let's check what verify.py said. It said 'sh600000'.
    assert data["data"]["market"] == "CN"

def test_search_stock(client):
    # Note: Search depends on local DB which is empty in test DB unless we populate it.
    # But the sync job runs on startup? 
    # In test environment, we use in-memory DB, so it's empty.
    # We should populate it first or mock the service.
    # Let's populate DB manually for test.
    from app.models.stock import Stock
    from app.db.session import SessionLocal # This would be the real DB, we need the test DB
    # The 'db' fixture provides the test session.
    # But we can't easily access it inside the test function if we use client fixture only?
    # We can request 'db' fixture in test function.
    pass

def test_search_stock_with_data(client, db):
    from app.models.stock import Stock
    # Insert dummy data
    stock = Stock(code="000001", symbol="000001", name="平安银行", market="CN", type="stock")
    db.add(stock)
    db.commit()

    response = client.get("/stock/search?name=平安")
    assert response.status_code == 200
    data = response.json()
    assert data["code"] == 0
    assert len(data["data"]) >= 1
    assert data["data"][0]["name"] == "平安银行"

def test_get_price_history(client):
    # This might fail if provider fails or if we don't have data.
    # But we are testing the API contract.
    # Ideally we should mock the provider to avoid external calls.
    # For integration test, we can call real provider.
    
    # Let's try a known date
    date = "2023-12-01"
    response = client.get(f"/stock/AAPL/price?date={date}")
    assert response.status_code == 200
    data = response.json()
    assert data["code"] == 0
    assert data["data"]["date"] == date
    assert "open" in data["data"]

def test_batch_get_prices(client):
    response = client.get("/stocks/price?symbols=AAPL,sh600000")
    assert response.status_code == 200
    data = response.json()
    assert data["code"] == 0
    assert len(data["data"]) == 2

def test_validation_error(client):
    response = client.get("/stock/AAPL/price") # Missing date
    assert response.status_code == 422
    data = response.json()
    assert data["code"] == 422
    assert "msg" in data

def test_404_error(client):
    response = client.get("/stock/INVALID_STOCK_SYMBOL_12345")
    # Provider might return None, API raises 404
    assert response.status_code == 404
    data = response.json()
    assert data["code"] == 404

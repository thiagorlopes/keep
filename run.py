from api_mock import create_app

app = create_app()

if __name__ == '__main__':
    print("Mock Flinks API server running on http://0.0.0.0:5000")
    app.run(debug=True, port=5000, host='0.0.0.0')

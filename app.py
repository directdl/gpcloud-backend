from gcloud_app import create_app

# Create WSGI app instance via factory
app = create_app()

if __name__ == '__main__':
    app.run(debug=False)

set -e
echo ">>> Pulling latest changes from the Git repository..."
git pull
echo ">>> Building the Go application..."
go build -o ncdot-ingester main.go
echo ">>> Build complete! Binary 'ncdot-ingester' is ready."